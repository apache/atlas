/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.store.graph.v1;


import com.google.common.base.Optional;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;

import java.util.Objects;

public class GraphMutationContext {


    /**
     * Atlas Attribute
     */

    private AtlasStructType.AtlasAttribute attribute;

    /**
     * Overriding type for which elements are being mapped
     */
    private AtlasType currentElementType;

    /**
     * Current attribute value/entity/Struct instance
     */
    private Object value;

    /**
     *
     * The vertex which corresponds to the entity/struct for which we are mapping a complex attributes like struct, traits
     */
    AtlasVertex referringVertex;

    /**
     * the vertex property that we are updating
     */

    String vertexPropertyKey;

    /**
     * The current edge(in case of updates) from the parent entity/struct to the complex attribute like struct, trait
     */
    Optional<AtlasEdge> existingEdge;


    private GraphMutationContext(final Builder builder) {
        this.attribute = builder.attribute;
        this.currentElementType = builder.elementType;
        this.existingEdge = builder.currentEdge;
        this.value = builder.currentValue;
        this.referringVertex = builder.referringVertex;
        this.vertexPropertyKey = builder.vertexPropertyKey;
    }

    public String getVertexPropertyKey() {
        return vertexPropertyKey;
    }

    @Override
    public int hashCode() {
        return Objects.hash(attribute, value, referringVertex, vertexPropertyKey, existingEdge);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        } else if (obj == this) {
            return true;
        } else if (obj.getClass() != getClass()) {
            return false;
        } else {
            GraphMutationContext rhs = (GraphMutationContext) obj;
            return Objects.equals(attribute, rhs.getAttribute())
                 && Objects.equals(value, rhs.getValue())
                 && Objects.equals(referringVertex, rhs.getReferringVertex())
                 && Objects.equals(vertexPropertyKey, rhs.getReferringVertex())
                 && Objects.equals(existingEdge, rhs.getCurrentEdge());
        }
    }


    public static final class Builder {

        private final AtlasStructType.AtlasAttribute attribute;

        private final AtlasType elementType;

        private final Object currentValue;

        private AtlasVertex referringVertex;

        private Optional<AtlasEdge> currentEdge = Optional.absent();

        private  String vertexPropertyKey;


        public Builder(AtlasStructType.AtlasAttribute attribute, AtlasType currentElementType, Object currentValue) {
            this.attribute = attribute;
            this.elementType = currentElementType;
            this.currentValue = currentValue;
        }

        public Builder(AtlasStructType.AtlasAttribute attribute, Object currentValue) {
            this.attribute = attribute;
            this.elementType = null;
            this.currentValue = currentValue;
        }

        Builder referringVertex(AtlasVertex referringVertex) {
            this.referringVertex = referringVertex;
            return this;
        }

        Builder edge(AtlasEdge edge) {
            this.currentEdge = Optional.of(edge);
            return this;
        }

        Builder edge(Optional<AtlasEdge> edge) {
            this.currentEdge = edge;
            return this;
        }

        Builder vertexProperty(String propertyKey) {
            this.vertexPropertyKey = propertyKey;
            return this;
        }

        GraphMutationContext build() {
            return new GraphMutationContext(this);
        }
    }

    public AtlasStructType getParentType() {
        return attribute.getStructType();
    }

    public AtlasStructDef getStructDef() {
        return attribute.getStructDef();
    }

    public AtlasStructDef.AtlasAttributeDef getAttributeDef() {
        return attribute.getAttributeDef();
    }

    public AtlasType getAttrType() {
        return currentElementType == null ? attribute.getAttributeType() : currentElementType;
    }

    public AtlasType getCurrentElementType() {
        return currentElementType;
    }

    public Object getValue() {
        return value;
    }

    public AtlasVertex getReferringVertex() {
        return referringVertex;
    }

    public Optional<AtlasEdge> getCurrentEdge() {
        return existingEdge;
    }

    public void setElementType(final AtlasType attrType) {
        this.currentElementType = attrType;
    }

    public AtlasStructType.AtlasAttribute getAttribute() {
        return attribute;
    }
}
