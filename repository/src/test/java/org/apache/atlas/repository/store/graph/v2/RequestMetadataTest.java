package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RequestMetadataTest {

    @AfterEach
    void tearDown() {
        RequestContext.clear();
    }

    @Test
    void testFromCurrentRequest_populatesAllFields() {
        RequestContext ctx = RequestContext.get();
        ctx.setTraceId("trace-abc-123");
        ctx.setUser("testuser", null);

        RequestMetadata rm = RequestMetadata.fromCurrentRequest();

        assertEquals("trace-abc-123", rm.getTraceId());
        assertEquals("testuser", rm.getUser());
    }

    @Test
    void testFromCurrentRequest_nullContext_handlesGracefully() {
        // RequestContext.get() returns a fresh context with null fields
        RequestContext ctx = RequestContext.get();
        // Don't set any fields — they should all be null

        RequestMetadata rm = RequestMetadata.fromCurrentRequest();

        assertNull(rm.getTraceId());
        // user may return null or a fallback, depending on RequestContext.getUser() impl
    }
}
