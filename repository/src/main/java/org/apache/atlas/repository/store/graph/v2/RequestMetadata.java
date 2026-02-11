package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.RequestContext;

/**
 * Simple POJO to carry request context into async ingestion Kafka events.
 * Populated from the ThreadLocal {@link RequestContext} on the HTTP thread.
 * <p>
 * Carries traceId, user, requestUri. The requestMethod is resolved by
 * {@link AsyncIngestionProducer#resolveHttpMethod(String)} based on eventType.
 */
public class RequestMetadata {
    private String traceId;
    private String user;
    private String requestUri;
    private String requestMethod;

    public RequestMetadata() {
    }

    /**
     * Build from the current ThreadLocal RequestContext.
     * Called from EntityMutationService / TypesREST (which run on the HTTP thread).
     * Note: requestMethod is set later by AsyncIngestionProducer based on eventType.
     */
    public static RequestMetadata fromCurrentRequest() {
        RequestContext ctx = RequestContext.get();
        RequestMetadata m = new RequestMetadata();
        m.traceId = ctx.getTraceId();
        m.user = ctx.getUser();
        m.requestUri = ctx.getRequestUri();
        // requestMethod is set by AsyncIngestionProducer based on eventType
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

    public String getRequestUri() {
        return requestUri;
    }

    public void setRequestUri(String requestUri) {
        this.requestUri = requestUri;
    }

    public String getRequestMethod() {
        return requestMethod;
    }

    public void setRequestMethod(String requestMethod) {
        this.requestMethod = requestMethod;
    }
}
