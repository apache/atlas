package org.apache.atlas.repository.graphdb.cassandra;

public enum RuntimeIdStrategy {
    LEGACY,
    DETERMINISTIC;

    public static RuntimeIdStrategy from(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return LEGACY;
        }

        String v = raw.trim().toLowerCase();
        if ("deterministic".equals(v) || "hash".equals(v) || "hash-identity".equals(v)) {
            return DETERMINISTIC;
        }

        return LEGACY;
    }
}

