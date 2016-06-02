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
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.filter.FilterFunctionPipe;
import org.apache.atlas.catalog.TermVertexWrapper;
import org.apache.atlas.catalog.VertexWrapper;
import org.apache.atlas.catalog.definition.EntityTagResourceDefinition;
import org.apache.atlas.catalog.definition.ResourceDefinition;
import org.apache.atlas.repository.Constants;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * Trait specific relation.
 */
//todo: combine with TagRelation
public class TraitRelation extends BaseRelation {
    //todo: for now using entity tag resource definition
    private static ResourceDefinition resourceDefinition = new EntityTagResourceDefinition();

    @Override
    public Collection<RelationSet> traverse(VertexWrapper vWrapper) {
        Vertex v = vWrapper.getVertex();
        Collection<VertexWrapper> vertices = new ArrayList<>();
        for (Edge e : v.getEdges(Direction.OUT)) {
            if (e.getLabel().startsWith(v.<String>getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY))) {
                VertexWrapper trait = new TermVertexWrapper(e.getVertex(Direction.IN));
                if (! trait.getPropertyKeys().contains("available_as_tag") && ! isDeleted(trait.getVertex())) {
                    vertices.add(trait);
                }
            }
        }
        return Collections.singletonList(new RelationSet("traits", vertices));
    }

    @Override
    public Pipe asPipe() {
        return new FilterFunctionPipe<>(new PipeFunction<Edge, Boolean>() {
            @Override
            public Boolean compute(Edge edge) {
                String name = edge.getVertex(Direction.OUT).getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY);
                if (edge.getLabel().startsWith(name)) {
                    VertexWrapper v = new TermVertexWrapper(edge.getVertex(Direction.IN));
                    return ! v.getPropertyKeys().contains("available_as_tag") && ! isDeleted(v.getVertex());
                } else {
                    return false;
                }
            }
        });
    }

    @Override
    public ResourceDefinition getResourceDefinition() {
        return resourceDefinition;
    }
}
