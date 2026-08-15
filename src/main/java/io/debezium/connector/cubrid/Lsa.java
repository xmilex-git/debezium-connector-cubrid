/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import java.util.Objects;

/**
 * A log sequence address of the CUBRID transaction log.
 * <p>
 * The {@code cubrid_log} C API hands out the LSA as a flat {@code uint64} whose raw value is
 * <em>not</em> monotonically ordered: the low 48 bits hold the page id and the high 16 bits hold
 * the offset within the page. Ordering therefore has to compare the {@code (pageId, offset)} tuple
 * and never the raw value (ADR 0004).
 */
public class Lsa implements Comparable<Lsa> {

    public static final Lsa NULL = new Lsa(-1L, -1L);

    private static final long PAGE_ID_MASK = 0x0000FFFFFFFFFFFFL;

    private final long pageId;
    private final long offset;

    public Lsa(long pageId, long offset) {
        this.pageId = pageId;
        this.offset = offset;
    }

    /**
     * Decomposes the flat {@code uint64} returned by the {@code cubrid_log} API.
     */
    public static Lsa fromRaw(long raw) {
        return new Lsa(raw & PAGE_ID_MASK, raw >>> 48);
    }

    /**
     * Recomposes the flat {@code uint64} accepted by the {@code cubrid_log} API.
     */
    public long toRaw() {
        return (offset << 48) | (pageId & PAGE_ID_MASK);
    }

    public long pageId() {
        return pageId;
    }

    public long offset() {
        return offset;
    }

    public boolean isAvailable() {
        return pageId >= 0;
    }

    @Override
    public int compareTo(Lsa other) {
        int byPage = Long.compare(pageId, other.pageId);
        return byPage != 0 ? byPage : Long.compare(offset, other.offset);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final Lsa other = (Lsa) obj;
        return pageId == other.pageId && offset == other.offset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pageId, offset);
    }

    @Override
    public String toString() {
        return "LSA(page=" + pageId + ",off=" + offset + ")";
    }
}
