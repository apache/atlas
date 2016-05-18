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

import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.transform.TransformFunctionPipe;
import org.apache.atlas.catalog.ResourceComparator;
import org.apache.atlas.catalog.VertexWrapper;

import java.util.*;

/**
 * Projection based on a relation.
 */
public class RelationProjection extends Projection {

    private Relation relation;
    public RelationProjection(String name, final Collection<String> fields, final Relation relation, Cardinality cardinality) {
        super(name, cardinality, new TransformFunctionPipe<>(
                new PipeFunction<VertexWrapper, Collection<ProjectionResult>>() {
                    @Override
                    public Collection<ProjectionResult> compute(VertexWrapper start) {
                        Collection<ProjectionResult> projectionResults = new ArrayList<>();

                        for (RelationSet relationSet : relation.traverse(start)) {
                            Collection<Map<String, Object>> propertyMaps = new ArrayList<>();

                            for (VertexWrapper vWrapper : relationSet.getVertices()) {
                                Map<String, Object> propertyMap = new TreeMap<>(new ResourceComparator());
                                propertyMaps.add(propertyMap);

                                if (fields.isEmpty()) {
                                    for (String property : vWrapper.getPropertyKeys()) {
                                        propertyMap.put(property, vWrapper.<String>getProperty(property));
                                    }
                                } else {
                                    for (String property : fields) {
                                        propertyMap.put(property, vWrapper.<String>getProperty(property));
                                    }
                                }
                            }
                            projectionResults.add(new ProjectionResult(relationSet.getName(), start, propertyMaps));
                        }
                        return projectionResults;
                    }
                }));
        this.relation = relation;
    }

    public Relation getRelation() {
        return relation;
    }
}
