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

package org.apache.atlas.catalog.query;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.filter.FilterFunctionPipe;
import org.apache.atlas.catalog.Request;
import org.apache.atlas.catalog.TermVertexWrapper;
import org.apache.atlas.catalog.VertexWrapper;
import org.apache.atlas.catalog.definition.EntityTagResourceDefinition;
import org.apache.atlas.catalog.definition.ResourceDefinition;
import org.apache.atlas.repository.Constants;

import java.util.HashMap;
import java.util.Map;

/**
 * Entity Tag resource query.
 */
public class AtlasEntityTagQuery extends BaseQuery {
    private final String guid;

    public AtlasEntityTagQuery(QueryExpression queryExpression, ResourceDefinition resourceDefinition, String guid, Request request) {
        super(queryExpression, resourceDefinition, request);
        this.guid = guid;
    }

    @Override
    protected Pipe getQueryPipe() {
        GremlinPipeline p =  new GremlinPipeline().has(Constants.GUID_PROPERTY_KEY, guid).outE();
        //todo: this is basically the same pipeline used in TagRelation.asPipe()
        p.add(new FilterFunctionPipe<>(new PipeFunction<Edge, Boolean>() {
            @Override
            public Boolean compute(Edge edge) {
                String type = edge.getVertex(Direction.OUT).getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY);
                VertexWrapper v = new TermVertexWrapper(edge.getVertex(Direction.IN));
                return edge.getLabel().startsWith(type) && v.getPropertyKeys().contains("available_as_tag");
            }
        }));

        return p.inV();
    }

    //todo: duplication of effort with resource definition
    @Override
    protected void addHref(Map<String, Object> propertyMap) {
        Map<String, Object> map = new HashMap<>(propertyMap);
        map.put(EntityTagResourceDefinition.ENTITY_GUID_PROPERTY, guid);
        String href = resourceDefinition.resolveHref(map);
        if (href != null) {
            propertyMap.put("href", href);
        }
    }
}
