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
import org.apache.atlas.catalog.Request;
import org.apache.atlas.catalog.ResourceComparator;
import org.apache.atlas.catalog.TermPath;
import org.apache.atlas.catalog.VertexWrapper;
import org.apache.atlas.catalog.exception.InvalidPayloadException;
import org.apache.atlas.catalog.projection.Projection;
import org.apache.atlas.catalog.projection.ProjectionResult;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.typesystem.types.*;
import org.apache.atlas.typesystem.types.utils.TypesUtil;

import java.util.*;

/**
 * Term resource definition.
 */
public class TermResourceDefinition extends BaseResourceDefinition {
    public TermResourceDefinition() {
        registerProperty(TypesUtil.createRequiredAttrDef("name", DataTypes.STRING_TYPE));
        registerProperty(TypesUtil.createOptionalAttrDef("description", DataTypes.STRING_TYPE));
        registerProperty(TypesUtil.createOptionalAttrDef("available_as_tag", DataTypes.BOOLEAN_TYPE));
        registerProperty(TypesUtil.createOptionalAttrDef("acceptable_use", DataTypes.STRING_TYPE));

        instanceProperties.add("name");
        instanceProperties.add("description");
        instanceProperties.add("creation_time");
        instanceProperties.add("available_as_tag");
        instanceProperties.add("acceptable_use");

        collectionProperties.add("name");
        collectionProperties.add("description");

        projections.put("terms", getSubTermProjection());
        projections.put("hierarchy", getHierarchyProjection());
    }

    @Override
    public void validate(Request request) throws InvalidPayloadException {
        super.validate(request);

        String name = request.getProperty("name");
        // name will be in the fully qualified form: taxonomyName.termName
        if (! name.contains(".")) {
            throw new InvalidPayloadException("Term name must be in the form 'taxonomyName.termName.subTermName'");
        }

        if (! request.getProperties().containsKey("available_as_tag")) {
            request.getProperties().put("available_as_tag", true);
        }
    }

    @Override
    public String getTypeName() {
        return "Term";
    }

    @Override
    public String getIdPropertyName() {
        return "name";
    }

    //todo
    @Override
    public String resolveHref(Map<String, Object> properties) {
        StringBuilder sb = new StringBuilder();
        sb.append("v1/taxonomies/");

        TermPath termPath = new TermPath(String.valueOf(properties.get("name")));
        String[] paths = termPath.getPathSegments();
        sb.append(termPath.getTaxonomyName());

        for (String path : paths) {
            //todo: shouldn't need to check for null or empty after TermPath addition
            if (path != null && !path.isEmpty()) {
                sb.append("/terms/");
                sb.append(path);
            }
        }

        return sb.toString();
    }

    private Projection getHierarchyProjection() {
        final String projectionName = "hierarchy";
        return new Projection(projectionName, Projection.Cardinality.SINGLE,
                new TransformFunctionPipe<>(new PipeFunction<VertexWrapper, Collection<ProjectionResult>>() {
                    @Override
                    public Collection<ProjectionResult> compute(VertexWrapper start) {
                        Map<String, Object> map = new TreeMap<>(new ResourceComparator());

                        TermPath termPath = new TermPath(start.getVertex().<String>getProperty(
                                Constants.ENTITY_TYPE_PROPERTY_KEY));

                        map.put("path", termPath.getPath());
                        map.put("short_name", termPath.getShortName());
                        map.put("taxonomy", termPath.getTaxonomyName());

                        return Collections.singleton(new ProjectionResult(projectionName, start,
                                Collections.singleton(map)));
                    }
                }));

    }

    private Projection getSubTermProjection() {
        //todo: combine with other term projections
        final String termsProjectionName = "terms";
        return new Projection(termsProjectionName, Projection.Cardinality.SINGLE,
                new TransformFunctionPipe<>(new PipeFunction<VertexWrapper, Collection<ProjectionResult>>() {
                    @Override
                    public Collection<ProjectionResult> compute(VertexWrapper start) {
                        Map<String, Object> map = new TreeMap<>(new ResourceComparator());

                        StringBuilder sb = new StringBuilder();
                        sb.append("v1/taxonomies/");

                        TermPath termPath = new TermPath(start.getVertex().<String>getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY));
                        String[] paths = termPath.getPathSegments();
                        sb.append(termPath.getTaxonomyName());

                        for (String path : paths) {
                            //todo: shouldn't need to check for null or empty after TermPath addition
                            if (path != null && !path.isEmpty()) {
                                sb.append("/terms/");
                                sb.append(path);
                            }
                        }
                        sb.append("/terms");

                        map.put("href", sb.toString());
                        return Collections.singleton(new ProjectionResult(termsProjectionName, start,
                                Collections.singleton(map)));
                    }
                }));
    }
}
