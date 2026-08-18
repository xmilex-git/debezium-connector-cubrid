/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * START_SESSION reply parsing (workspace#70): a current server appends the node facts
 * (HA server state + db_creation) to the success acknowledgment; a bare 4-byte success
 * reply is the pre-dictionary wire format and must stop with an explicit version error
 * (ADR 0011 D10 — lockstep shipping, no fallback, no negotiation).
 */
class StartSessionReplyTest {

    @Test
    void extendedReplyCarriesTheNodeFacts() {
        byte[] reply = new OrWriter()
                .writeInt(0)
                .writeString("active")
                .writeInt64(1_755_000_000L)
                .toByteArray();

        CubridLogClient.NodeFacts facts = CubridLogClient.parseStartSessionReply(reply);
        assertEquals("active", facts.haServerState());
        assertEquals(1_755_000_000L, facts.dbCreationSeconds());
    }

    @Test
    void bareSuccessReplyIsAnExplicitTooOldError() {
        byte[] oldFormat = new OrWriter().writeInt(0).toByteArray();

        CubridLogException e = assertThrows(CubridLogException.class,
                () -> CubridLogClient.parseStartSessionReply(oldFormat));
        assertEquals(CubridLogException.FAILED_CONNECT, e.returnCode());
        assertTrue(e.getMessage().contains("predates"), "the error names the version mismatch, not a generic failure");
    }

    @Test
    void errorRepliesKeepTheirClassification() {
        byte[] unavailable = new OrWriter().writeInt(WireConstants.ER_CDC_NOT_AVAILABLE).toByteArray();
        assertEquals(CubridLogException.UNAVAILABLE_CDC_SERVER,
                assertThrows(CubridLogException.class, () -> CubridLogClient.parseStartSessionReply(unavailable)).returnCode());

        byte[] other = new OrWriter().writeInt(-1).toByteArray();
        assertEquals(CubridLogException.FAILED_CONNECT,
                assertThrows(CubridLogException.class, () -> CubridLogClient.parseStartSessionReply(other)).returnCode());
    }
}
