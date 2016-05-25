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
import org.apache.atlas.RequestContext;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.TypeSystem;

import static org.apache.atlas.repository.Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.STATE_PROPERTY_KEY;

public class SoftDeleteHandler extends DeleteHandler {
    @Inject
    public SoftDeleteHandler(TypeSystem typeSystem) {
        super(typeSystem, false, true);
    }

    @Override
    protected void _deleteVertex(Vertex instanceVertex, boolean force) {
        if (force) {
            graphHelper.removeVertex(instanceVertex);
        } else {
            Id.EntityState state = GraphHelper.getState(instanceVertex);
            if (state != Id.EntityState.DELETED) {
                GraphHelper.setProperty(instanceVertex, STATE_PROPERTY_KEY, Id.EntityState.DELETED.name());
                GraphHelper.setProperty(instanceVertex, MODIFICATION_TIMESTAMP_PROPERTY_KEY,
                        RequestContext.get().getRequestTime());
            }
        }
    }

    @Override
    protected void deleteEdge(Edge edge, boolean force) throws AtlasException {
        if (force) {
            graphHelper.removeEdge(edge);
        } else {
            Id.EntityState state = GraphHelper.getState(edge);
            if (state != Id.EntityState.DELETED) {
                GraphHelper.setProperty(edge, STATE_PROPERTY_KEY, Id.EntityState.DELETED.name());
                GraphHelper
                        .setProperty(edge, MODIFICATION_TIMESTAMP_PROPERTY_KEY, RequestContext.get().getRequestTime());
            }
        }
    }
}
