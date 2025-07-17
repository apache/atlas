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
package org.janusgraph.diskstorage.rdbms;

import org.janusgraph.graphdb.relations.RelationIdentifier;

public class RdbmsUniqueKeyHandler {
    private static final String SQL_INSERT_UNIQUE_VERTEX_KEY                   = "INSERT INTO janus_unique_vertex_key (id, vertex_id, key_name, val) VALUES (NEXTVAL('janus_unique_vertex_key_seq'), ?, ?, ?)";
    private static final String SQL_DELETE_UNIQUE_VERTEX_KEY                   = "DELETE FROM janus_unique_vertex_key WHERE key_name = ? AND val = ?";
    private static final String SQL_INSERT_UNIQUE_VERTEX_TYPE_KEY              = "INSERT INTO janus_unique_vertex_type_key (id, vertex_id, type_name, key_name, val) VALUES (NEXTVAL('janus_unique_vertex_type_key_seq'), ?, ?, ?, ?)";
    private static final String SQL_DELETE_UNIQUE_VERTEX_TYPE_KEY              = "DELETE FROM janus_unique_vertex_type_key WHERE type_name = ? AND key_name = ? AND val = ?";
    private static final String SQL_DELETE_UNIQUE_VERTEX_KEY_BY_VERTEX_ID      = "DELETE FROM janus_unique_vertex_key WHERE vertex_id = ?";
    private static final String SQL_DELETE_UNIQUE_VERTEX_TYPE_KEY_BY_VERTEX_ID = "DELETE FROM janus_unique_vertex_type_key WHERE vertex_id = ?";

    private static final String SQL_INSERT_UNIQUE_EDGE_KEY                 = "INSERT INTO janus_unique_edge_key (id, edge_id, key_name, val) VALUES (NEXTVAL('janus_unique_edge_key_seq'), ?, ?, ?)";
    private static final String SQL_DELETE_UNIQUE_EDGE_KEY                 = "DELETE FROM janus_unique_edge_key WHERE key_name = ? AND val = ?";
    private static final String SQL_INSERT_UNIQUE_EDGE_TYPE_KEY            = "INSERT INTO janus_unique_edge_type_key (id, edge_id, type_name, key_name, val) VALUES (NEXTVAL('janus_unique_edge_type_key_seq'), ?, ?, ?, ?)";
    private static final String SQL_DELETE_UNIQUE_EDGE_TYPE_KEY            = "DELETE FROM janus_unique_edge_type_key WHERE type_name = ? AND key_name = ? AND val = ?";
    private static final String SQL_DELETE_UNIQUE_EDGE_KEY_BY_EDGE_ID      = "DELETE FROM janus_unique_edge_key WHERE edge_id = ?";
    private static final String SQL_DELETE_UNIQUE_EDGE_TYPE_KEY_BY_EDGE_ID = "DELETE FROM janus_unique_edge_type_key WHERE edge_id = ?";

    public void addUniqueKey(String keyName, Object value, Object elementId, boolean isVertex) {
        RdbmsTransaction trx = RdbmsTransaction.getActiveTransaction();

        if (trx != null) {
            trx.getEntityManager().createNativeQuery(isVertex ? SQL_INSERT_UNIQUE_VERTEX_KEY : SQL_INSERT_UNIQUE_EDGE_KEY)
                    .setParameter(1, getNumberId(elementId))
                    .setParameter(2, keyName)
                    .setParameter(3, value)
                    .executeUpdate();
        } else {
            throw new IllegalStateException("No active transaction found to add unique key: keyName=" + keyName + ", value=" + value);
        }
    }

    public void removeUniqueKey(String keyName, Object value, Object elementId, boolean isVertex) {
        RdbmsTransaction trx = RdbmsTransaction.getActiveTransaction();

        if (trx != null) {
            trx.getEntityManager().createNativeQuery(isVertex ? SQL_DELETE_UNIQUE_VERTEX_KEY : SQL_DELETE_UNIQUE_EDGE_KEY)
                    .setParameter(1, getNumberId(elementId))
                    .setParameter(2, keyName)
                    .setParameter(3, value)
                    .executeUpdate();
        } else {
            throw new IllegalStateException("No active transaction found to remove unique key: keyName=" + keyName + ", value=" + value);
        }
    }

    public void addTypeUniqueKey(String typeName, String keyName, Object value, Object elementId, boolean isVertex) {
        RdbmsTransaction trx = RdbmsTransaction.getActiveTransaction();

        if (trx != null) {
            trx.getEntityManager().createNativeQuery(isVertex ? SQL_INSERT_UNIQUE_VERTEX_TYPE_KEY : SQL_INSERT_UNIQUE_EDGE_TYPE_KEY)
                    .setParameter(1, getNumberId(elementId))
                    .setParameter(2, typeName)
                    .setParameter(3, keyName)
                    .setParameter(4, value)
                    .executeUpdate();
        } else {
            throw new IllegalStateException("No active transaction found to add type unique key: typeName=" + typeName + ", keyName=" + keyName + ", value=" + value);
        }
    }

    public void removeTypeUniqueKey(String typeName, String keyName, Object value, Object elementId, boolean isVertex) {
        RdbmsTransaction trx = RdbmsTransaction.getActiveTransaction();

        if (trx != null) {
            trx.getEntityManager().createNativeQuery(isVertex ? SQL_DELETE_UNIQUE_VERTEX_TYPE_KEY : SQL_DELETE_UNIQUE_EDGE_TYPE_KEY)
                    .setParameter(1, getNumberId(elementId))
                    .setParameter(2, typeName)
                    .setParameter(3, keyName)
                    .setParameter(4, value)
                    .executeUpdate();
        } else {
            throw new IllegalStateException("No active transaction found to remove type unique key: typeName=" + typeName + ", keyName=" + keyName + ", value=" + value);
        }
    }

    public void removeUniqueKeysForVertexId(Object vertexId) {
        RdbmsTransaction trx = RdbmsTransaction.getActiveTransaction();

        if (trx != null) {
            final Number id = getNumberId(vertexId);

            trx.getEntityManager().createNativeQuery(SQL_DELETE_UNIQUE_VERTEX_KEY_BY_VERTEX_ID)
                    .setParameter(1, id)
                    .executeUpdate();

            trx.getEntityManager().createNativeQuery(SQL_DELETE_UNIQUE_VERTEX_TYPE_KEY_BY_VERTEX_ID)
                    .setParameter(1, id)
                    .executeUpdate();
        } else {
            throw new IllegalStateException("No active transaction found to remove unique keys for vertex: vertexId=" + vertexId);
        }
    }

    public void removeUniqueKeysForEdgeId(Object edgeId) {
        RdbmsTransaction trx = RdbmsTransaction.getActiveTransaction();

        if (trx != null) {
            final Number id = getNumberId(edgeId);

            trx.getEntityManager().createNativeQuery(SQL_DELETE_UNIQUE_EDGE_KEY_BY_EDGE_ID)
                    .setParameter(1, id)
                    .executeUpdate();

            trx.getEntityManager().createNativeQuery(SQL_DELETE_UNIQUE_EDGE_TYPE_KEY_BY_EDGE_ID)
                    .setParameter(1, id)
                    .executeUpdate();
        } else {
            throw new IllegalStateException("No active transaction found to remove unique keys for edge: edgeId=" + edgeId);
        }
    }

    private Number getNumberId(Object elementId) {
        if (elementId instanceof Number) {
            return (Number) elementId;
        } else if (elementId instanceof RelationIdentifier) {
            return ((RelationIdentifier) elementId).getRelationId();
        } else if (elementId == null) {
            throw new IllegalArgumentException("Invalid elementId: null");
        } else {
            throw new IllegalArgumentException("Invalid elementId type: " + elementId.getClass().getName());
        }
    }
}
