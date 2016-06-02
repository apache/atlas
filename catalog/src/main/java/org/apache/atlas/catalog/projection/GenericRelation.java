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

package org.apache.atlas.catalog.projection;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.pipes.Pipe;
import org.apache.atlas.catalog.VertexWrapper;
import org.apache.atlas.catalog.definition.ResourceDefinition;
import org.apache.atlas.repository.Constants;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a generic relation
 */
public class GenericRelation extends BaseRelation {
    private final ResourceDefinition resourceDefinition;

    public GenericRelation(ResourceDefinition resourceDefinition) {
        this.resourceDefinition = resourceDefinition;
    }

    @Override
    public Collection<RelationSet> traverse(VertexWrapper vWrapper) {
        Collection<RelationSet> relations = new ArrayList<>();
        Vertex v = vWrapper.getVertex();
        String vertexType = v.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY);
        Map<String, Collection<VertexWrapper>> vertexMap = new HashMap<>();
        for (Edge e : v.getEdges(Direction.OUT)) {
            String edgeLabel = e.getLabel();
            String edgePrefix = String.format("%s%s.", Constants.INTERNAL_PROPERTY_KEY_PREFIX, vertexType);
            if (edgeLabel.startsWith(edgePrefix)) {
                Vertex adjacentVertex = e.getVertex(Direction.IN);
                if (! isDeleted(adjacentVertex)) {
                    VertexWrapper relationVertex = new VertexWrapper(adjacentVertex, resourceDefinition);
                    String relationName = edgeLabel.substring(edgePrefix.length());
                    Collection<VertexWrapper> vertices = vertexMap.get(relationName);
                    if (vertices == null) {
                        vertices = new ArrayList<>();
                        vertexMap.put(relationName, vertices);
                    }
                    vertices.add(relationVertex);
                }
            }
        }
        for (Map.Entry<String, Collection<VertexWrapper>> entry : vertexMap.entrySet()) {
            relations.add(new RelationSet(entry.getKey(), entry.getValue()));
        }
        return relations;
    }

    @Override
    public Pipe asPipe() {
        return null;
    }

    @Override
    public ResourceDefinition getResourceDefinition() {
        return resourceDefinition;
    }
}
