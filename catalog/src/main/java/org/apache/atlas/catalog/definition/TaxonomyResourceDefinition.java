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
import org.apache.atlas.AtlasConstants;
import org.apache.atlas.catalog.Request;
import org.apache.atlas.catalog.TaxonomyResourceProvider;
import org.apache.atlas.catalog.VertexWrapper;
import org.apache.atlas.catalog.exception.InvalidPayloadException;
import org.apache.atlas.catalog.projection.Projection;
import org.apache.atlas.catalog.projection.ProjectionResult;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.utils.TypesUtil;

import java.util.*;

/**
 * Taxonomy resource definition.
 */
public class TaxonomyResourceDefinition extends BaseResourceDefinition {
    public TaxonomyResourceDefinition() {
        registerProperty(TypesUtil.createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE));
        registerProperty(TypesUtil.createOptionalAttrDef("description", DataTypes.STRING_TYPE));
        registerProperty(TypesUtil.createOptionalAttrDef(TaxonomyResourceProvider.NAMESPACE_ATTRIBUTE_NAME, DataTypes.STRING_TYPE));

        //todo: combine with above registrations
        instanceProperties.add("name");
        instanceProperties.add("description");
        instanceProperties.add("creation_time");

        collectionProperties.add("name");
        collectionProperties.add("description");

        projections.put("terms", getTermsProjection());
    }

    @Override
    public void validateCreatePayload(Request request) throws InvalidPayloadException {
        super.validateCreatePayload(request);
        if (String.valueOf(request.getQueryProperties().get("name")).contains(".")) {
            throw new InvalidPayloadException("The \"name\" property may not contain the character '.'");
        }
    }

    @Override
    public String getTypeName() {
        return "Taxonomy";
    }

    @Override
    public String getIdPropertyName() {
        return "name";
    }

    @Override
    public String resolveHref(Map<String, Object> properties) {
        return String.format("v1/taxonomies/%s", properties.get("name"));
    }

    private Projection getTermsProjection() {
        final String termsProjectionName = "terms";
        return new Projection(termsProjectionName, Projection.Cardinality.SINGLE,
                new TransformFunctionPipe<>(new PipeFunction<VertexWrapper, Collection<ProjectionResult>>() {
                    private String baseHref = "v1/taxonomies/";
                    @Override
                    public Collection<ProjectionResult> compute(VertexWrapper v) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("href", baseHref + v.getProperty("name") + "/terms");
                        return Collections.singleton(new ProjectionResult(termsProjectionName, v,
                                Collections.singleton(map)));
                    }
                }));
    }
}
