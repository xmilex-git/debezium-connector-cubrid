/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/**
 * {@link JdbcConnection} extension to be used with CUBRID.
 * <p>
 * The schema part of every connector-level {@link TableId} is the CUBRID <b>owner</b> (ADR 0011
 * D8, revising ADR 0006 D5): CUBRID's two-level namespace is {@code owner.table}, which puts the
 * owner in the standard Debezium topic slot {@code <prefix>.<schemaName>.<tableName>} exactly like
 * Oracle/PG. Schema discovery goes through the PUBLIC catalog views {@code db_class} /
 * {@code db_attribute} / {@code db_index(_key)} with an owner filter (ADR 0011 D9) instead of the
 * driver's {@link java.sql.DatabaseMetaData} — the driver ignores catalog/schema arguments
 * entirely (ADR 0005) and would silently merge the columns of same-named tables across owners.
 * The views filter rows by the caller's privileges, matching the per-table SELECT authorization
 * model of ADR 0011 D1.
 * <p>
 * Owner names are stored uppercase in the catalog; the connector normalizes them to lowercase in
 * table ids, topics and include lists — the same normal form CUBRID applies to identifiers.
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
     * Owner-qualified quoted form {@code "owner"."table"} (ADR 0011 D8 ③). Qualification is what
     * lets a non-DBA account address another owner's granted table at all: a bare name resolves
     * against the caller's own namespace and fails with {@code Unknown class "<user>.<table>"}.
     */
    @Override
    public String quotedTableIdString(TableId tableId) {
        return QUOTED_CHARACTER + tableId.schema() + QUOTED_CHARACTER
                + "." + QUOTED_CHARACTER + tableId.table() + QUOTED_CHARACTER;
    }

    /**
     * Reads the relational model of one {@code owner.table} from the PUBLIC catalog views
     * (ADR 0011 D9). The owner filter is what makes same-named tables under different owners
     * fully distinct — the previous driver-metadata path merged their columns silently.
     */
    public Optional<Table> readTable(TableId tableId) throws SQLException {
        final List<Column> columns = new ArrayList<>();
        try (PreparedStatement ps = connection().prepareStatement(
                "SELECT attr_name, data_type, prec, scale, is_nullable, def_order, default_value"
                        + " FROM db_attribute"
                        + " WHERE owner_name = UPPER(?) AND class_name = ? AND attr_type = 'INSTANCE'"
                        + " ORDER BY def_order")) {
            ps.setString(1, tableId.schema());
            ps.setString(2, tableId.table());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    final String typeName = rs.getString(2);
                    final ColumnEditor column = Column.editor()
                            .name(rs.getString(1))
                            .type(typeName)
                            .jdbcType(jdbcTypeFor(typeName))
                            .length(rs.getInt(3))
                            .scale(rs.getInt(4))
                            .optional("YES".equalsIgnoreCase(rs.getString(5)))
                            .position(rs.getInt(6) + 1);
                    final String defaultValue = rs.getString(7);
                    if (defaultValue != null) {
                        column.defaultValueExpression(defaultValue);
                    }
                    columns.add(column.create());
                }
            }
        }
        if (columns.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(Table.editor()
                .tableId(tableId)
                .addColumns(columns)
                .setPrimaryKeyNames(readPrimaryKeyNames(tableId))
                .create());
    }

    /**
     * Reads the database codeset id — the engine's {@code INTL_CODESET} enum value — from
     * {@code db_root}, for the UTF-8-only startup guard (workspace#77). {@code db_root} is
     * PUBLIC-selectable (measured on 11.5, non-DBA account), and the stored id is immune to
     * session state such as {@code SET NAMES}, unlike a {@code CHARSET(<literal>)} probe.
     */
    public int readDatabaseCharsetId() throws SQLException {
        try (PreparedStatement ps = connection().prepareStatement("SELECT charset FROM db_root");
                ResultSet rs = ps.executeQuery()) {
            if (!rs.next()) {
                throw new SQLException("db_root returned no row while reading the database charset");
            }
            return rs.getInt(1);
        }
    }

    private List<String> readPrimaryKeyNames(TableId tableId) throws SQLException {
        final List<String> pkNames = new ArrayList<>();
        try (PreparedStatement ps = connection().prepareStatement(
                "SELECT k.key_attr_name"
                        + " FROM db_index i, db_index_key k"
                        + " WHERE i.owner_name = UPPER(?) AND i.class_name = ? AND i.is_primary_key = 'YES'"
                        + " AND k.owner_name = i.owner_name AND k.class_name = i.class_name"
                        + " AND k.index_name = i.index_name"
                        + " ORDER BY k.key_order")) {
            ps.setString(1, tableId.schema());
            ps.setString(2, tableId.table());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    pkNames.add(rs.getString(1));
                }
            }
        }
        return pkNames;
    }

    /**
     * Maps a {@code db_attribute.data_type} string to a {@code java.sql.Types} constant, mirroring
     * what the JDBC driver reports for the same column (measured — {@code docs/type-support.md}).
     * The decoder and the value converters key off this jdbcType; the typeName keeps the catalog
     * string, which is what distinguishes MONETARY from DOUBLE and JSON from STRING (the
     * unsupported-type guard of ADR 0008's spirit must use the typeName, never the jdbcType).
     */
    static int jdbcTypeFor(String dataType) {
        switch (dataType.toUpperCase(Locale.ROOT)) {
            case "SHORT":
                return Types.SMALLINT;
            case "INTEGER":
                return Types.INTEGER;
            case "BIGINT":
                return Types.BIGINT;
            case "NUMERIC":
                return Types.NUMERIC;
            case "FLOAT":
                return Types.REAL;
            case "DOUBLE":
            case "MONETARY":
                return Types.DOUBLE;
            case "CHAR":
                return Types.CHAR;
            case "STRING":
            case "ENUM":
            case "JSON":
                return Types.VARCHAR;
            case "DATE":
                return Types.DATE;
            case "TIME":
                return Types.TIME;
            case "TIMESTAMP":
            case "DATETIME":
            case "TIMESTAMPTZ":
            case "TIMESTAMPLTZ":
            case "DATETIMETZ":
            case "DATETIMELTZ":
                return Types.TIMESTAMP;
            case "BIT":
                return Types.BINARY;
            case "VARBIT":
                return Types.VARBINARY;
            case "BLOB":
                return Types.BLOB;
            case "CLOB":
                return Types.CLOB;
            default: // SET / MULTISET / SEQUENCE and anything the matrix does not know
                return Types.OTHER;
        }
    }

    /**
     * Enumerates the user tables visible to the caller, owner-qualified (ADR 0011 D8). The PUBLIC
     * view filters rows by the caller's privileges, so a restricted account only sees what it was
     * granted.
     */
    public java.util.Set<TableId> readUserTableIds() throws SQLException {
        final java.util.Set<TableId> tableIds = new java.util.HashSet<>();
        try (java.sql.Statement stmt = connection().createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT owner_name, class_name FROM db_class WHERE is_system_class = 'NO' AND class_type = 'CLASS'")) {
            while (rs.next()) {
                tableIds.add(new TableId(null, rs.getString(1).toLowerCase(Locale.ROOT), rs.getString(2)));
            }
        }
        return tableIds;
    }
}
