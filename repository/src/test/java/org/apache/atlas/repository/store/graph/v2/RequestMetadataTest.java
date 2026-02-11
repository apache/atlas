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
        ctx.setUri("/api/atlas/v2/entity/bulk");

        RequestMetadata rm = RequestMetadata.fromCurrentRequest();

        assertEquals("trace-abc-123", rm.getTraceId());
        assertEquals("testuser", rm.getUser());
        assertEquals("/api/atlas/v2/entity/bulk", rm.getRequestUri());
        assertNull(rm.getRequestMethod(), "requestMethod should be null — set later by AsyncIngestionProducer");
    }

    @Test
    void testFromCurrentRequest_nullContext_handlesGracefully() {
        // RequestContext.get() returns a fresh context with null fields
        RequestContext ctx = RequestContext.get();
        // Don't set any fields — they should all be null

        RequestMetadata rm = RequestMetadata.fromCurrentRequest();

        assertNull(rm.getTraceId());
        assertNull(rm.getRequestUri());
        assertNull(rm.getRequestMethod());
        // user may return null or a fallback, depending on RequestContext.getUser() impl
    }

    @Test
    void testSetRequestMethod() {
        RequestMetadata rm = new RequestMetadata();
        assertNull(rm.getRequestMethod());
        rm.setRequestMethod("POST");
        assertEquals("POST", rm.getRequestMethod());
    }
}
