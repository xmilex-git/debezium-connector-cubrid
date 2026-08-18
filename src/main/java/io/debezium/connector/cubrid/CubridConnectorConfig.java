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

    public static final Field TRANSACTION_EVENTS_THRESHOLD = Field.create("transaction.events.threshold")
            .withDisplayName("Transaction events threshold")
            .withType(ConfigDef.Type.LONG)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 1))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("The maximum number of events a single transaction may buffer before the "
                    + "transaction is abandoned: its buffer is discarded, its remaining events are skipped, "
                    + "and its changes are permanently lost downstream (recovery requires a re-snapshot). "
                    + "Use 0 (the default) to buffer transactions of unlimited size (ADR 0007 D2).")
            .withValidation(Field::isNonNegativeLong)
            .withDefault(0L);

    public static final Field TRANSACTION_RETENTION_MS = Field.create("transaction.retention.ms")
            .withDisplayName("Transaction retention (ms)")
            .withType(ConfigDef.Type.LONG)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 2))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("The maximum time in milliseconds an in-flight transaction may stay buffered "
                    + "before it is abandoned and the restart anchor is advanced past it, so a long-running "
                    + "transaction cannot pin the anchor beyond the supplemental log retention. Abandoned "
                    + "changes are permanently lost downstream (recovery requires a re-snapshot). "
                    + "Use 0 (the default) to retain in-flight transactions indefinitely (ADR 0007 D3).")
            .withValidation(Field::isNonNegativeLong)
            .withDefault(0L);

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

    /**
     * Test-only fault-injection hooks (ADR 0009 D2 fault test ②): pause the snapshot for the
     * given ms immediately before the barrier LSA capture / immediately after the barrier is
     * captured and the pre-barrier REPEATABLE READ view is discarded, so a test can inject
     * commits deterministically into each window. 0 (default) = no pause; never set in production.
     */
    public static final Field SNAPSHOT_TEST_PAUSE_BEFORE_BARRIER_MS = Field.createInternal("snapshot.test.pause.before.barrier.ms")
            .withType(ConfigDef.Type.LONG)
            .withValidation(Field::isNonNegativeLong)
            .withDefault(0L);

    public static final Field SNAPSHOT_TEST_PAUSE_AFTER_BARRIER_MS = Field.createInternal("snapshot.test.pause.after.barrier.ms")
            .withType(ConfigDef.Type.LONG)
            .withValidation(Field::isNonNegativeLong)
            .withDefault(0L);

    private static final ConfigDefinition CONFIG_DEFINITION = RelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
            .name("CUBRID")
            .group(Field.Group.CONNECTION,
                    HOSTNAME,
                    PORT,
                    USER,
                    PASSWORD,
                    DATABASE_NAME,
                    CDC_PORT)
            .group(Field.Group.CONNECTOR,
                    SNAPSHOT_MODE,
                    TRANSACTION_EVENTS_THRESHOLD,
                    TRANSACTION_RETENTION_MS)
            .group(Field.Group.CONNECTOR_ADVANCED, SOURCE_INFO_STRUCT_MAKER)
            .create();

    protected static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static final Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    /**
     * The literal {@code owner.table} form required of every {@code table.include.list} entry
     * (ADR 0011 D2/D8): the entries double as the server-side extraction target names and the
     * per-table SELECT authorization list, so regex patterns — which the server could not
     * resolve and the privilege probe could not check — are rejected at startup.
     */
    private static final java.util.regex.Pattern INCLUDE_LIST_ENTRY = java.util.regex.Pattern.compile("[A-Za-z0-9_#]+\\.[A-Za-z0-9_#]+");

    private final String databaseName;
    private final SnapshotMode snapshotMode;
    private final int cdcPort;
    private final long transactionEventsThreshold;
    private final long transactionRetentionMs;
    private final long snapshotTestPauseBeforeBarrierMs;
    private final long snapshotTestPauseAfterBarrierMs;
    private final java.util.List<String> extractionTableNames;

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
        this.transactionEventsThreshold = config.getLong(TRANSACTION_EVENTS_THRESHOLD);
        this.transactionRetentionMs = config.getLong(TRANSACTION_RETENTION_MS);
        this.snapshotTestPauseBeforeBarrierMs = config.getLong(SNAPSHOT_TEST_PAUSE_BEFORE_BARRIER_MS);
        this.snapshotTestPauseAfterBarrierMs = config.getLong(SNAPSHOT_TEST_PAUSE_AFTER_BARRIER_MS);
        this.extractionTableNames = parseExtractionTableNames(config.getString(TABLE_INCLUDE_LIST));
    }

    /**
     * {@code table.include.list} is mandatory (ADR 0011 D2): an unset list means a whole-log
     * session, which stays DBA-only, and both the per-table SELECT check (D1) and the relation
     * dictionary scope (D5) are derived from it.
     */
    private static java.util.List<String> parseExtractionTableNames(String tableIncludeList) {
        if (tableIncludeList == null || tableIncludeList.isBlank()) {
            throw new io.debezium.DebeziumException(
                    "'" + TABLE_INCLUDE_LIST.name() + "' is required: the CUBRID connector derives its capture "
                            + "targets, per-table SELECT authorization and relation dictionary scope from it "
                            + "(ADR 0011 D2). Capturing the whole log without a list is not supported.");
        }
        final java.util.List<String> names = new java.util.ArrayList<>();
        for (String entry : tableIncludeList.split(",")) {
            final String name = entry.trim();
            if (name.isEmpty()) {
                continue;
            }
            if (!INCLUDE_LIST_ENTRY.matcher(name).matches()) {
                throw new io.debezium.DebeziumException(
                        "'" + TABLE_INCLUDE_LIST.name() + "' entry '" + name + "' is not a literal owner.table name. "
                                + "The CUBRID connector passes each entry verbatim to the server as an extraction "
                                + "target and probes SELECT on it, so regex patterns are not supported (ADR 0011 D2/D8).");
            }
            names.add(name.toLowerCase(java.util.Locale.ROOT));
        }
        if (names.isEmpty()) {
            throw new io.debezium.DebeziumException(
                    "'" + TABLE_INCLUDE_LIST.name() + "' contains no usable owner.table entry (ADR 0011 D2).");
        }
        return java.util.List.copyOf(names);
    }

    /**
     * The literal {@code owner.table} capture targets (lowercase normal form), fed to the
     * server as name-based extraction targets (ADR 0011 D3) and to the CDC authorization
     * gate as the per-table SELECT list (D1).
     */
    public java.util.List<String> getExtractionTableNames() {
        return extractionTableNames;
    }

    /** Test-only pause before the snapshot barrier capture; 0 = none (ADR 0009 D2 ②). */
    public long getSnapshotTestPauseBeforeBarrierMs() {
        return snapshotTestPauseBeforeBarrierMs;
    }

    /** Test-only pause after the barrier capture + view discard; 0 = none (ADR 0009 D2 ②). */
    public long getSnapshotTestPauseAfterBarrierMs() {
        return snapshotTestPauseAfterBarrierMs;
    }

    /** Per-transaction buffered-event cap; 0 = unlimited (ADR 0007 D2). */
    public long getTransactionEventsThreshold() {
        return transactionEventsThreshold;
    }

    /** Max in-flight transaction age in ms before abandon; 0 = unlimited (ADR 0007 D3). */
    public long getTransactionRetentionMs() {
        return transactionRetentionMs;
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
        // The POC snapshot never locks — write stop is an operator procedure (ADR 0005 D2).
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
