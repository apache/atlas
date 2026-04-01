/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.store.graph.v2;

/**
 * Standalone replacement for org.janusgraph.util.encoding.LongEncoding.
 * Encodes/decodes long values to/from compact string representation
 * compatible with JanusGraph's Elasticsearch document ID format.
 *
 * The encoding uses base-36 (digits 0-9 then lowercase a-z) matching
 * JanusGraph's actual LongEncoding implementation (janusgraph-driver).
 * This is a direct reimplementation to remove the JanusGraph dependency
 * while maintaining backward compatibility with existing ES document IDs.
 */
public final class LongEncodingUtil {

    private static final String BASE_SYMBOLS = "0123456789abcdefghijklmnopqrstuvwxyz";
    private static final int    BASE         = BASE_SYMBOLS.length(); // 36

    private LongEncodingUtil() {
        // utility class
    }

    /**
     * Encode a long value to a compact string representation.
     * Matches JanusGraph's LongEncoding.encode() output exactly.
     */
    public static String encode(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("Expected non-negative value: " + value);
        }
        if (value == 0) {
            return String.valueOf(BASE_SYMBOLS.charAt(0));
        }

        StringBuilder sb = new StringBuilder();
        while (value > 0) {
            sb.append(BASE_SYMBOLS.charAt((int) (value % BASE)));
            value = value / BASE;
        }
        return sb.reverse().toString();
    }

    /**
     * Decode a string back to a long value.
     * Matches JanusGraph's LongEncoding.decode() output exactly.
     */
    public static long decode(String encoded) {
        long value = 0;
        for (int i = 0; i < encoded.length(); i++) {
            int digit = BASE_SYMBOLS.indexOf(encoded.charAt(i));
            if (digit < 0) {
                throw new IllegalArgumentException("Invalid character in encoded string: " + encoded.charAt(i));
            }
            value = value * BASE + digit;
        }
        return value;
    }

    /**
     * Compute the ES document ID from a vertex ID string.
     *
     * Two cases:
     * 1. JanusGraph (existing tenants): vertex IDs are numeric longs (e.g., "4096").
     *    The ES doc ID is the base-36 encoding (e.g., "38g"), matching JanusGraph's
     *    LongEncoding.encode() output. This preserves backward compatibility with
     *    existing ES indices.
     *
     * 2. Cassandra backend (new tenants): vertex IDs are UUIDs
     *    (e.g., "550e8400-e29b-41d4-a716-446655440000"). The ES doc ID is the UUID
     *    returned as-is, matching the _id that CassandraGraph.appendESIndexAction()
     *    writes to Elasticsearch during commit.
     */
    public static String vertexIdToDocId(String vertexId) {
        try {
            return encode(Long.parseLong(vertexId));
        } catch (NumberFormatException e) {
            // Non-numeric vertex ID (e.g., UUID from Cassandra graph backend)
            // Return as-is to match the _id used by CassandraGraph's ES sync
            return vertexId;
        }
    }
}
