package org.apache.atlas.repository.graphdb.migrator;

public enum IdStrategy {
    LEGACY,
    HASH_JG,
    HASH_IDENTITY;   // hash(typeName + "|" + qualifiedName) — matches runtime GraphIdUtil

    public static IdStrategy from(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return LEGACY;
        }

        String v = raw.trim().toLowerCase();
        if ("hash-jg".equals(v) || "hash_jg".equals(v)) {
            return HASH_JG;
        }
        if ("deterministic".equals(v) || "hash-identity".equals(v) || "hash_identity".equals(v)) {
            return HASH_IDENTITY;
        }

        return LEGACY;
    }
}

