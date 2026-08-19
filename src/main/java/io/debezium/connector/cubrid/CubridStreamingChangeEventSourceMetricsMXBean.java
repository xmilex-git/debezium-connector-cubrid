/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import java.util.Set;

import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetricsMXBean;

/**
 * CUBRID-specific streaming metrics exposed over JMX (ADR 0007): observability for the
 * per-transaction buffer policy. {@code OldestInflightAgeInMilliseconds} is the alerting
 * signal — an old in-flight transaction pins the restart anchor and can outlive the
 * supplemental log retention (the CUBRID counterpart of Oracle's
 * {@code OldestScnAgeInMilliseconds}).
 */
public interface CubridStreamingChangeEventSourceMetricsMXBean extends StreamingChangeEventSourceMetricsMXBean {

    /** Number of transactions currently buffered in flight. */
    long getNumberOfActiveTransactions();

    /** Number of transactions abandoned because they exceeded {@code transaction.events.threshold} (D2). */
    long getNumberOfOversizedTransactions();

    /** Number of transactions abandoned because they exceeded {@code transaction.retention.ms} (D3). */
    long getAbandonedTransactionCount();

    /** Most recently abandoned transaction ids (both D2 and D3), newest last, bounded. */
    Set<String> getAbandonedTransactionIds();

    /** Age in ms of the oldest in-flight transaction's first buffered change; 0 when none in flight. */
    long getOldestInflightAgeInMilliseconds();

    /** Number of DDL halts fired on captured tables (ADR 0008 D5); each one fails the task. */
    long getDdlHaltCount();

    /** Captured table of the most recent DDL halt; empty when none fired. */
    String getLastDdlHaltTable();

    /** DDL type and statement of the most recent DDL halt; empty when none fired. */
    String getLastDdlHaltStatement();

    /** Number of mid-stream CREATE TABLE items observed and skipped (ADR 0008 D3). */
    long getMidStreamCreateTableCount();

    /** Number of HA halts fired (ADR 0010 D2); each one fails the task. */
    long getHaHaltCount();

    /** Message of the most recent HA halt; empty when none fired. */
    String getLastHaHaltReason();

    /** Number of empty/half-empty relation-announce halts (workspace#82 D2); each one fails the task. */
    long getEmptyAnnounceHaltCount();

    /** Classoid of the most recent empty-announce halt; empty when none fired. */
    String getLastEmptyAnnounceHaltClassoid();

    /** Number of announce↔include-list mismatch halts (workspace#82 D5); each one fails the task. */
    long getAnnounceIncludeMismatchHaltCount();

    /** {@code owner.table} of the most recent announce↔include mismatch halt; empty when none fired. */
    String getLastAnnounceIncludeMismatchTable();
}
