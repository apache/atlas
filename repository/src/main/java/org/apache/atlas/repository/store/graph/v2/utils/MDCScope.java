package org.apache.atlas.repository.store.graph.v2.utils;

import org.slf4j.MDC;

import java.util.Map;

public final class MDCScope implements AutoCloseable {
    private final Map<String, String> old;
    public MDCScope(Map<String, String> kv) {
        old = MDC.getCopyOfContextMap();
        kv.forEach((k, v) -> { if (v != null) MDC.put(k, v); });
    }
    @Override public void close() {
        MDC.clear();
        if (old != null) old.forEach(MDC::put);
    }
    public static MDCScope of(String k, String v) { return new MDCScope(Map.of(k, v)); }
}
