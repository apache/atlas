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

package org.apache.atlas.catalog.definition;

import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.transform.TransformFunctionPipe;
import org.apache.atlas.catalog.DefaultPropertyMapper;
import org.apache.atlas.catalog.PropertyMapper;
import org.apache.atlas.catalog.ResourceComparator;
import org.apache.atlas.catalog.VertexWrapper;
import org.apache.atlas.catalog.projection.Projection;
import org.apache.atlas.catalog.projection.ProjectionResult;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.utils.TypesUtil;

import java.util.*;

/**
 * Entity Tag resource definition.
 */
public class EntityTagResourceDefinition extends BaseResourceDefinition {
    public static final String ENTITY_GUID_PROPERTY = "entity-guid";

    public EntityTagResourceDefinition() {
        registerProperty(TypesUtil.createRequiredAttrDef("name", DataTypes.STRING_TYPE));

        instanceProperties.add("name");
        instanceProperties.add("description");
        instanceProperties.add("creation_time");

        collectionProperties.add("name");
        collectionProperties.add("description");

        projections.put("terms", getTermProjection());
    }

    @Override
    public String getIdPropertyName() {
        return "name";
    }

    //not meaningful for entity tags
    @Override
    public String getTypeName() {
        return null;
    }

    @Override
    public String resolveHref(Map<String, Object> properties) {
        return String.format("v1/entities/%s/tags/%s", properties.get(ENTITY_GUID_PROPERTY), properties.get("name"));
    }

    private Projection getTermProjection() {
        return new Projection("term", Projection.Cardinality.SINGLE,
                new TransformFunctionPipe<>(new PipeFunction<VertexWrapper, Collection<ProjectionResult>>() {
                    @Override
                    public Collection<ProjectionResult> compute(VertexWrapper start) {
                        Map<String, Object> map = new TreeMap<>(new ResourceComparator());

                        StringBuilder sb = new StringBuilder();
                        sb.append("v1/taxonomies/");

                        String fullyQualifiedName = start.getVertex().getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY);
                        String[] paths = fullyQualifiedName.split("\\.");
                        // first path segment is the taxonomy
                        sb.append(paths[0]);

                        for (int i = 1; i < paths.length; ++i) {
                            String path = paths[i];
                            if (path != null && !path.isEmpty()) {
                                sb.append("/terms/");
                                sb.append(path);
                            }
                        }

                        map.put("href", sb.toString());
                        return Collections.singleton(new ProjectionResult("term", start,
                                Collections.singleton(map)));
                    }
                }));
    }

    @Override
    protected PropertyMapper createPropertyMapper() {
        return new DefaultPropertyMapper(Collections.singletonMap(Constants.ENTITY_TYPE_PROPERTY_KEY, "name"),
                Collections.singletonMap("name", Constants.ENTITY_TYPE_PROPERTY_KEY));
    }
}
