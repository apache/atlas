package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.RequestContext;

/**
 * Simple POJO to carry request context into async ingestion Kafka events.
 * Populated from the ThreadLocal {@link RequestContext} on the HTTP thread.
 *
 * Only carries traceId and user — the consumer routes based on eventType,
 * so HTTP method/URI are not needed.
 */
public class RequestMetadata {
    private String traceId;
    private String user;

    public RequestMetadata() {
    }

    /**
     * Build from the current ThreadLocal RequestContext.
     * Called from EntityMutationService / TypesREST (which run on the HTTP thread).
     */
    public static RequestMetadata fromCurrentRequest() {
        RequestContext ctx = RequestContext.get();
        RequestMetadata m = new RequestMetadata();
        m.traceId = ctx.getTraceId();
        m.user = ctx.getUser();
        return m;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }
}
