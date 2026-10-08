/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.repository.graphdb;

public abstract class AtlasUniqueKeyHandler {
    public abstract void addUniqueKey(String keyName, Object value, Object elementId, boolean isVertex);

    public abstract void removeUniqueKey(String keyName, Object value, boolean isVertex);

    public abstract void addTypeUniqueKey(String typeName, String keyName, Object value, Object elementId, boolean isVertex);

    public abstract void removeTypeUniqueKey(String typeName, String keyName, Object value, boolean isVertex);

    public abstract void removeUniqueKeysForVertexId(Object vertexId);

    public abstract void removeUniqueKeysForEdgeId(Object edgeId);

    /**
     * Whether {@link #findVertexIdByUniqueKey(String, Object)} can answer from this backend's
     * uniqueness table on the graph transaction that is already open.  RDBMS can, by reading
     * {@code janus_unique_vertex_key} on that Postgres transaction; other backends leave claim
     * lookups on the graph query.
     *
     * <p>A {@code true} result is authoritative, including when the lookup returns {@code null}:
     * no row means nobody holds the key, and the caller must not fall through to a graph scan.
     */
    public boolean supportsUniqueKeyLookup() {
        return false;
    }

    /**
     * Vertex id that currently holds {@code keyName}={@code value}, or {@code null} if none does.
     * {@code null} is "nobody holds it" only when {@link #supportsUniqueKeyLookup()} is {@code true}.
     */
    public Object findVertexIdByUniqueKey(String keyName, Object value) {
        return null;
    }
}
