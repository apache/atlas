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

import org.apache.atlas.repository.graphdb.AtlasUniqueKeyHandler;
import org.janusgraph.diskstorage.rdbms.RdbmsUniqueKeyHandler;

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
        uniqueKeyHandler.addTypeUniqueKey(typeName, keyName, value, elementId, isVertex);
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
}
