/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.repository.graph;

import com.google.inject.Inject;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.types.TypeSystem;

public class HardDeleteHandler extends DeleteHandler {

    @Inject
    public HardDeleteHandler(TypeSystem typeSystem) {
        super(typeSystem, true, false);
    }

    @Override
    protected void _deleteVertex(Vertex instanceVertex, boolean force) {
        graphHelper.removeVertex(instanceVertex);
    }

    @Override
    protected void deleteEdge(Edge edge, boolean force) throws AtlasException {
        graphHelper.removeEdge(edge);
    }
}
