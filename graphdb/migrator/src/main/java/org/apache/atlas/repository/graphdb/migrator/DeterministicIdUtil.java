package org.apache.atlas.repository.graphdb.migrator;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class DeterministicIdUtil {
    private DeterministicIdUtil() {
    }

    public static String vertexIdFromJg(long jgVertexId, IdStrategy strategy) {
        if (strategy == IdStrategy.HASH_JG) {
            return hash32("v|" + jgVertexId);
        }
        return String.valueOf(jgVertexId);
    }

    public static String edgeIdFromJg(long jgRelationId, IdStrategy strategy) {
        if (strategy == IdStrategy.HASH_JG) {
            return hash32("e|" + jgRelationId);
        }
        return String.valueOf(jgRelationId);
    }

    public static String vertexIdFromIdentity(String typeName, String qualifiedName) {
        String identityKey = buildIdentityKey(typeName, qualifiedName);
        if (identityKey == null) {
            return null;
        }
        return hash32("v|" + identityKey);
    }

    public static String edgeIdFromIdentity(String outVertexId, String label, String inVertexId) {
        return hash32("e|" + outVertexId + "|" + label + "|" + inVertexId);
    }

    public static String buildIdentityKey(String typeName, String qualifiedName) {
        if (typeName == null || qualifiedName == null) {
            return null;
        }

        String t = typeName.trim();
        String q = qualifiedName.trim();
        if (t.isEmpty() || q.isEmpty()) {
            return null;
        }

        return t + "\u001F" + q;
    }

    static String hash32(String s) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(64);
            for (byte b : bytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.substring(0, 32);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}

