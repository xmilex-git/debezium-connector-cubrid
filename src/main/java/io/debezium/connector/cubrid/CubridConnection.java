/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import cubrid.sql.CUBRIDOID;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.ColumnNameFilter;

/**
 * {@link JdbcConnection} extension to be used with CUBRID.
 * <p>
 * The CUBRID driver ignores the catalog/schema arguments of every {@link DatabaseMetaData} call
 * (ADR 0005), so all lookups here go by bare table name and the connector-level {@link TableId}
 * uses the logical database name as its schema part (workspace#40 D1) — which also makes the
 * default topic naming {@code <prefix>.<db>.<table>} match the sink contract of workspace#39.
 */
public class CubridConnection extends JdbcConnection {

    private static final String DRIVER_CLASS_NAME = "cubrid.jdbc.driver.CUBRIDDriver";

    private static final String QUOTED_CHARACTER = "\"";

    private static final String URL_PATTERN = "jdbc:cubrid:${"
            + JdbcConfiguration.HOSTNAME + "}:${"
            + JdbcConfiguration.PORT + "}:${"
            + JdbcConfiguration.DATABASE + "}:::";

    private static final ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory(
            URL_PATTERN,
            DRIVER_CLASS_NAME,
            CubridConnection.class.getClassLoader(),
            JdbcConfiguration.PORT.withDefault(CubridConnectorConfig.PORT.defaultValueAsString()));

    public CubridConnection(JdbcConfiguration config) {
        super(config, FACTORY, QUOTED_CHARACTER, QUOTED_CHARACTER);
    }

    /**
     * @return a JDBC connection string for the current configuration
     */
    public String connectionString() {
        return connectionString(URL_PATTERN);
    }

    /**
     * The owner schema of a {@link TableId} here is the logical database name, not a CUBRID
     * schema, so qualification would not parse — quote the bare table name only (POC runs as
     * the owning user).
     */
    @Override
    public String quotedTableIdString(TableId tableId) {
        return QUOTED_CHARACTER + tableId.table() + QUOTED_CHARACTER;
    }

    /**
     * The 11.3 driver returns a JDBC-3.0-shaped (18 column) {@code getColumns} result set:
     * {@code IS_AUTOINCREMENT} (index 23) does not exist and the unguarded read of the base
     * implementation throws (ADR 0005 — this override is mandatory).
     */
    @Override
    protected Optional<ColumnEditor> readTableColumn(ResultSet columnMetadata, TableId tableId, ColumnNameFilter columnFilter) throws SQLException {
        final int metaColumnCount = columnMetadata.getMetaData().getColumnCount();
        final String defaultValue = columnMetadata.getString(13);

        final String columnName = columnMetadata.getString(4);
        if (columnFilter == null || columnFilter.matches(tableId.catalog(), tableId.schema(), tableId.table(), columnName)) {
            ColumnEditor column = Column.editor().name(columnName);
            column.type(columnMetadata.getString(6));
            column.length(columnMetadata.getInt(7));
            if (columnMetadata.getObject(9) != null) {
                column.scale(columnMetadata.getInt(9));
            }
            column.optional(isNullable(columnMetadata.getInt(11)));
            column.position(columnMetadata.getInt(17));
            if (metaColumnCount >= 23) {
                column.autoIncremented("YES".equalsIgnoreCase(columnMetadata.getString(23)));
            }
            column.nativeType(resolveNativeType(column.typeName()));
            column.jdbcType(resolveJdbcType(columnMetadata.getInt(5), column.nativeType()));
            if (defaultValue != null) {
                column.defaultValueExpression(defaultValue);
            }
            return Optional.of(column);
        }
        return Optional.empty();
    }

    /**
     * Reads the relational model of one table by bare name and re-homes it under the given
     * {@link TableId} (whose schema part is the logical database name).
     */
    public Optional<Table> readTable(TableId tableId) throws SQLException {
        final DatabaseMetaData metadata = connection().getMetaData();
        final List<Column> columns = new ArrayList<>();
        try (ResultSet rs = metadata.getColumns(null, null, tableId.table(), null)) {
            while (rs.next()) {
                readTableColumn(rs, tableId, null).ifPresent(editor -> columns.add(editor.create()));
            }
        }
        if (columns.isEmpty()) {
            return Optional.empty();
        }
        columns.sort(Comparator.comparingInt(Column::position));
        final List<String> pkNames = readPrimaryKeyNames(metadata, tableId);
        return Optional.of(Table.editor()
                .tableId(tableId)
                .addColumns(columns)
                .setPrimaryKeyNames(pkNames)
                .create());
    }

    /**
     * Enumerates the user tables from the catalog, re-homed under the logical database name
     * (the driver ignores catalog/schema metadata filters entirely — ADR 0005).
     */
    public java.util.Set<TableId> readUserTableIds(String logicalDatabase) throws SQLException {
        final java.util.Set<TableId> tableIds = new java.util.HashSet<>();
        try (java.sql.Statement stmt = connection().createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT class_name FROM db_class WHERE is_system_class = 'NO' AND class_type = 'CLASS'")) {
            while (rs.next()) {
                tableIds.add(new TableId(null, logicalDatabase, rs.getString(1)));
            }
        }
        return tableIds;
    }

    /**
     * Maps the {@code classoid} carried by every {@code cubrid_log} DML item to its table name.
     * <p>
     * The engine emits the classoid as the raw 8-byte memcpy of its {@code OID} struct
     * ({@code pageid int32 | slotid int16 | volid int16}, little-endian), and the JDBC driver
     * exposes the same OID through {@code _db_class.class_of} as {@code @pageid|slotid|volid} —
     * verified against the P0 harness dumps (workspace#40).
     */
    public Map<Long, String> readClassOidMap() throws SQLException {
        final Map<Long, String> map = new HashMap<>();
        try (java.sql.Statement stmt = connection().createStatement();
                ResultSet rs = stmt.executeQuery("SELECT class_of, class_name FROM _db_class")) {
            while (rs.next()) {
                final Object oid = rs.getObject(1);
                final String name = rs.getString(2);
                if (oid instanceof CUBRIDOID cubridOid) {
                    final String[] parts = cubridOid.getOidString().substring(1).split("\\|");
                    final long pageId = Long.parseLong(parts[0]);
                    final long slotId = Long.parseLong(parts[1]);
                    final long volId = Long.parseLong(parts[2]);
                    map.put((volId << 48) | (slotId << 32) | pageId, name);
                }
            }
        }
        return map;
    }
}
