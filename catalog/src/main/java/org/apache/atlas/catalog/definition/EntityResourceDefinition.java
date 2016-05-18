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
import org.apache.atlas.catalog.exception.InvalidPayloadException;
import org.apache.atlas.catalog.projection.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Entity resource definition.
 */
public class EntityResourceDefinition extends BaseResourceDefinition {
    public EntityResourceDefinition() {
        collectionProperties.add("name");
        collectionProperties.add("id");
        collectionProperties.add("type");

        RelationProjection tagProjection = getTagProjection();
        projections.put("tags", tagProjection);
        RelationProjection traitProjection = getTraitProjection();
        projections.put("traits", traitProjection);
        projections.put("default", getDefaultRelationProjection());

        relations.put(tagProjection.getName(), tagProjection.getRelation());
        relations.put(traitProjection.getName(), traitProjection.getRelation());
    }

    @Override
    public String getIdPropertyName() {
        return "id";
    }

    // not meaningful for entities
    @Override
    public String getTypeName() {
        return null;
    }

    @Override
    public void validate(Request request) throws InvalidPayloadException {
        // no op for entities as we don't currently create entities and
        // each entity type is different
    }

    @Override
    public String resolveHref(Map<String, Object> properties) {
        Object id = properties.get("id");
        return id == null ? null : String.format("v1/entities/%s", id);
    }

    private RelationProjection getTagProjection() {
        Relation traitRelation = new TagRelation();
        RelationProjection tagProjection = new RelationProjection("tags", Collections.singleton("name"),
                traitRelation, Projection.Cardinality.MULTIPLE);
        tagProjection.addPipe(new TransformFunctionPipe<>(
                new PipeFunction<Collection<ProjectionResult>, Collection<ProjectionResult>>() {
                    @Override
                    public Collection<ProjectionResult> compute(Collection<ProjectionResult> results) {
                        for (ProjectionResult result : results) {
                            for (Map<String, Object> properties : result.getPropertyMaps()) {
                                properties.put("href", String.format("v1/entities/%s/tags/%s",
                                        result.getStartingVertex().getProperty("id"), properties.get("name")));
                            }
                        }
                        return results;
                    }
                }));
        return tagProjection;
    }

    private RelationProjection getTraitProjection() {
        return new RelationProjection("traits", Collections.<String>emptySet(),
                new TraitRelation(), Projection.Cardinality.MULTIPLE);
    }

    private RelationProjection getDefaultRelationProjection() {
        Relation genericRelation = new GenericRelation(this);
        RelationProjection relationProjection = new RelationProjection(
                "relations",
                Arrays.asList("type", "id", "name"),
                genericRelation, Projection.Cardinality.MULTIPLE);

        relationProjection.addPipe(new TransformFunctionPipe<>(
                new PipeFunction<Collection<ProjectionResult>, Collection<ProjectionResult>>() {
                    @Override
                    public Collection<ProjectionResult> compute(Collection<ProjectionResult> results) {
                        for (ProjectionResult result : results) {
                            for (Map<String, Object> properties : result.getPropertyMaps()) {
                                properties.put("href", String.format("v1/entities/%s", properties.get("id")));
                            }
                        }
                        return results;
                    }
                }));
        return relationProjection;
    }
}
