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
package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.repository.graphdb.AtlasSchemaViolationException;
import org.apache.atlas.repository.graphdb.AtlasUniqueKeyHandler;
import org.janusgraph.diskstorage.rdbms.RdbmsUniqueKeyHandler;

import java.sql.SQLException;

public class AtlasJanusRdbmsUniqueKeyHandler extends AtlasUniqueKeyHandler {
    private final RdbmsUniqueKeyHandler uniqueKeyHandler;

    public AtlasJanusRdbmsUniqueKeyHandler() {
        uniqueKeyHandler = new RdbmsUniqueKeyHandler();
    }

    @Override
    public void addUniqueKey(String keyName, Object value, Object elementId, boolean isVertex) {
        uniqueKeyHandler.addUniqueKey(keyName, value, elementId, isVertex);
    }

    @Override
    public void removeUniqueKey(String keyName, Object value, boolean isVertex) {
        uniqueKeyHandler.removeUniqueKey(keyName, value, isVertex);
    }

    @Override
    public void addTypeUniqueKey(String typeName, String keyName, Object value, Object elementId, boolean isVertex) {
        try {
            uniqueKeyHandler.addTypeUniqueKey(typeName, keyName, value, elementId, isVertex);
        } catch (RuntimeException exception) {
            // A duplicate (type, key, value) is a race with a create that already owns the row.
            // Surface it as a schema violation so ingest rolls back and retries the lookup.
            if (isUniqueViolation(exception)) {
                throw new AtlasSchemaViolationException(exception);
            }

            throw exception;
        }
    }

    private static boolean isUniqueViolation(Throwable throwable) {
        for (Throwable cause = throwable; cause != null; cause = nextCause(cause)) {
            if (cause instanceof SQLException && "23505".equals(((SQLException) cause).getSQLState())) {
                return true;
            }

            String message = cause.getMessage();

            if (message != null && (message.contains("23505") || message.contains("janus_unique_vertex_type_key_uk") || message.contains("janus_unique_edge_type_key_uk"))) {
                return true;
            }
        }

        return false;
    }

    private static Throwable nextCause(Throwable cause) {
        Throwable next = cause.getCause();

        return next == cause ? null : next;
    }

    @Override
    public void removeTypeUniqueKey(String typeName, String keyName, Object value, boolean isVertex) {
        uniqueKeyHandler.removeTypeUniqueKey(typeName, keyName, value, isVertex);
    }

    @Override
    public void removeUniqueKeysForVertexId(Object vertexId) {
        uniqueKeyHandler.removeUniqueKeysForVertexId(vertexId);
    }

    @Override
    public void removeUniqueKeysForEdgeId(Object edgeId) {
        uniqueKeyHandler.removeUniqueKeysForEdgeId(edgeId);
    }

    @Override
    public boolean supportsUniqueKeyLookup() {
        return uniqueKeyHandler.supportsUniqueKeyLookup();
    }

    @Override
    public Object findVertexIdByUniqueKey(String keyName, Object value) {
        return uniqueKeyHandler.findVertexIdByUniqueKey(keyName, value);
    }

    @Override
    public Object findVertexIdByTypeUniqueKey(String typeName, String keyName, Object value) {
        return uniqueKeyHandler.findVertexIdByTypeUniqueKey(typeName, keyName, value);
    }
}
