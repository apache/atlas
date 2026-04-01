package org.apache.atlas.repository.graphdb.cassandra;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class GraphIdUtil {
    private GraphIdUtil() {
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

    public static String deterministicVertexId(String identityKey) {
        return hash32("v|" + identityKey);
    }

    public static String deterministicEdgeId(String outVertexId, String label, String inVertexId) {
        return hash32("e|" + outVertexId + "|" + label + "|" + inVertexId);
    }

    private static String hash32(String s) {
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

