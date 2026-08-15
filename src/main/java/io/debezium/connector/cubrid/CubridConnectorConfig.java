/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import java.util.Optional;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Width;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;

/**
 * The list of configuration options for the CUBRID connector.
 * <p>
 * The connector is <em>not</em> historized: schema is read from the database on start rather than
 * replayed from a schema history topic, following the Postgres model.
 */
public class CubridConnectorConfig extends RelationalDatabaseConnectorConfig {

    protected static final int DEFAULT_PORT = 33000;

    /** Default {@code cubrid_port_id} master port used by the {@code cubrid_log} API. */
    protected static final int DEFAULT_CDC_PORT = 1523;

    protected static final int DEFAULT_SNAPSHOT_FETCH_SIZE = 2_000;

    /**
     * The set of predefined SnapshotMode options.
     */
    public enum SnapshotMode implements EnumeratedValue {

        /**
         * Perform a snapshot of the schema and data upon initial startup of the connector.
         */
        INITIAL("initial"),

        /**
         * Perform a snapshot of the schema but no data upon initial startup of the connector.
         */
        NO_DATA("no_data");

        private final String value;

        SnapshotMode(String value) {
            this.value = value;
        }

        public static SnapshotMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (SnapshotMode option : SnapshotMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        public static SnapshotMode parse(String value, String defaultValue) {
            SnapshotMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    public static final Field PORT = RelationalDatabaseConnectorConfig.PORT.withDefault(DEFAULT_PORT);

    public static final Field CDC_PORT = Field.create("cdc.port")
            .withDisplayName("CDC master port")
            .withType(ConfigDef.Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 0))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("The cubrid_port_id master port the cubrid_log CDC API connects to. "
                    + "This is distinct from the broker port used by the JDBC connection.")
            .withValidation(Field::isPositiveInteger)
            .withDefault(DEFAULT_CDC_PORT);

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 0))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("The criteria for running a snapshot upon startup of the connector. "
                    + "Options include: "
                    + "'initial' (the default) to take a snapshot of schema and data; "
                    + "'no_data' to take a snapshot of the schema only.");

    public static final Field SOURCE_INFO_STRUCT_MAKER = CommonConnectorConfig.SOURCE_INFO_STRUCT_MAKER
            .withDefault(CubridSourceInfoStructMaker.class.getName());

    private static final ConfigDefinition CONFIG_DEFINITION = RelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
            .name("CUBRID")
            .type(
                    HOSTNAME,
                    PORT,
                    USER,
                    PASSWORD,
                    DATABASE_NAME,
                    CDC_PORT)
            .connector(
                    SNAPSHOT_MODE)
            .events(SOURCE_INFO_STRUCT_MAKER)
            .create();

    protected static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static final Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    private final String databaseName;
    private final SnapshotMode snapshotMode;
    private final int cdcPort;

    public CubridConnectorConfig(Configuration config) {
        super(
                config,
                new SystemTablesPredicate(),
                t -> t.schema() + '.' + t.table(),
                DEFAULT_SNAPSHOT_FETCH_SIZE,
                ColumnFilterMode.SCHEMA,
                false);

        this.databaseName = config.getString(DATABASE_NAME);
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE), SNAPSHOT_MODE.defaultValueAsString());
        this.cdcPort = config.getInteger(CDC_PORT);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public SnapshotMode getSnapshotMode() {
        return snapshotMode;
    }

    @Override
    public Optional<? extends EnumeratedValue> getSnapshotLockingMode() {
        // The POC snapshot never locks; see TODO(workspace#39).
        return Optional.empty();
    }

    public int getCdcPort() {
        return cdcPort;
    }

    @Override
    protected SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(Version version) {
        return getSourceInfoStructMaker(SOURCE_INFO_STRUCT_MAKER, Module.name(), Module.version(), this);
    }

    @Override
    public String getContextName() {
        return Module.contextName();
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }

    private static class SystemTablesPredicate implements TableFilter {

        @Override
        public boolean isIncluded(TableId t) {
            return !t.table().toLowerCase().startsWith("db_")
                    && !t.table().toLowerCase().startsWith("_db_");
        }
    }
}
