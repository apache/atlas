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
package org.apache.atlas.model.instance;


import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.NONE;
import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.PUBLIC_ONLY;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class EntityMutationResponse {

    Map<EntityMutations.EntityOperation, List<AtlasEntityHeader>> mutatedEntities;
    Map<String,String>                                            guidAssignments;

    public EntityMutationResponse() {
    }

    public EntityMutationResponse(final Map<EntityMutations.EntityOperation, List<AtlasEntityHeader>> mutatedEntities) {
        this.mutatedEntities = mutatedEntities;
    }

    public Map<EntityMutations.EntityOperation, List<AtlasEntityHeader>> getMutatedEntities() {
        return mutatedEntities;
    }

    public void setMutatedEntities(final Map<EntityMutations.EntityOperation, List<AtlasEntityHeader>> mutatedEntities) {
        this.mutatedEntities = mutatedEntities;
    }

    public void setGuidAssignments(Map<String,String> guidAssignments) {
        this.guidAssignments = guidAssignments;
    }

    public Map<String,String> getGuidAssignments() {
        return guidAssignments;
    }


    @JsonIgnore
    public List<AtlasEntityHeader> getEntitiesByOperation(EntityMutations.EntityOperation op) {
        if ( mutatedEntities != null) {
            return mutatedEntities.get(op);
        }
        return null;
    }

    @JsonIgnore
    public List<AtlasEntityHeader> getCreatedEntities() {
        if ( mutatedEntities != null) {
            return mutatedEntities.get(EntityMutations.EntityOperation.CREATE);
        }
        return null;
    }

    @JsonIgnore
    public List<AtlasEntityHeader> getUpdatedEntities() {
        if ( mutatedEntities != null) {
            return mutatedEntities.get(EntityMutations.EntityOperation.UPDATE);
        }
        return null;
    }

    @JsonIgnore
    public List<AtlasEntityHeader> getDeletedEntities() {
        if ( mutatedEntities != null) {
            return mutatedEntities.get(EntityMutations.EntityOperation.DELETE);
        }
        return null;
    }

    @JsonIgnore
    public AtlasEntityHeader getFirstEntityCreated() {
        final List<AtlasEntityHeader> entitiesByOperation = getEntitiesByOperation(EntityMutations.EntityOperation.CREATE);
        if ( entitiesByOperation != null && entitiesByOperation.size() > 0) {
            return entitiesByOperation.get(0);
        }

        return null;
    }

    @JsonIgnore
    public AtlasEntityHeader getFirstEntityUpdated() {
        final List<AtlasEntityHeader> entitiesByOperation = getEntitiesByOperation(EntityMutations.EntityOperation.UPDATE);
        if ( entitiesByOperation != null && entitiesByOperation.size() > 0) {
            return entitiesByOperation.get(0);
        }

        return null;
    }

    @JsonIgnore
    public AtlasEntityHeader getFirstCreatedEntityByTypeName(String typeName) {
        return getFirstEntityByType(getEntitiesByOperation(EntityMutations.EntityOperation.CREATE), typeName);
    }

    @JsonIgnore
    public AtlasEntityHeader getFirstDeletedEntityByTypeName(String typeName) {
        return getFirstEntityByType(getEntitiesByOperation(EntityMutations.EntityOperation.DELETE), typeName);
    }

    @JsonIgnore
    public List<AtlasEntityHeader> getCreatedEntitiesByTypeName(String typeName) {
        return getEntitiesByType(getEntitiesByOperation(EntityMutations.EntityOperation.CREATE), typeName);
    }

    @JsonIgnore
    public AtlasEntityHeader getCreatedEntityByTypeNameAndAttribute(String typeName, String attrName, String attrVal) {
        return getEntityByTypeAndUniqueAttribute(getEntitiesByOperation(EntityMutations.EntityOperation.CREATE), typeName, attrName, attrVal);
    }

    @JsonIgnore
    public AtlasEntityHeader getUpdatedEntityByTypeNameAndAttribute(String typeName, String attrName, String attrVal) {
        return getEntityByTypeAndUniqueAttribute(getEntitiesByOperation(EntityMutations.EntityOperation.UPDATE), typeName, attrName, attrVal);
    }

    @JsonIgnore
    public List<AtlasEntityHeader> getUpdatedEntitiesByTypeName(String typeName) {
        return getEntitiesByType(getEntitiesByOperation(EntityMutations.EntityOperation.UPDATE), typeName);
    }

    @JsonIgnore
    public List<AtlasEntityHeader> getDeletedEntitiesByTypeName(String typeName) {
        return getEntitiesByType(getEntitiesByOperation(EntityMutations.EntityOperation.DELETE), typeName);
    }

    @JsonIgnore
    public AtlasEntityHeader getFirstUpdatedEntityByTypeName(String typeName) {
        return getFirstEntityByType(getEntitiesByOperation(EntityMutations.EntityOperation.UPDATE), typeName);
    }

    public void addEntity(EntityMutations.EntityOperation op, AtlasEntityHeader header) {
        if (mutatedEntities == null) {
            mutatedEntities = new HashMap<>();
        }

        List<AtlasEntityHeader> opEntities = mutatedEntities.get(op);

        if (opEntities == null) {
            opEntities = new ArrayList<>();
            mutatedEntities.put(op, opEntities);
        }

        opEntities.add(header);
    }


    public StringBuilder toString(StringBuilder sb) {
        if ( sb == null) {
            sb = new StringBuilder();
        }

        AtlasBaseTypeDef.dumpObjects(mutatedEntities, sb);

        return sb;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EntityMutationResponse that = (EntityMutationResponse) o;
        return Objects.equals(mutatedEntities, that.mutatedEntities) &&
               Objects.equals(guidAssignments, that.guidAssignments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mutatedEntities, guidAssignments);
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }

    private AtlasEntityHeader getFirstEntityByType(List<AtlasEntityHeader> entitiesByOperation, String typeName) {
        if ( entitiesByOperation != null && entitiesByOperation.size() > 0) {
            for (AtlasEntityHeader header : entitiesByOperation) {
                if ( header.getTypeName().equals(typeName)) {
                    return header;
                }
            }
        }
        return null;
    }

    private List<AtlasEntityHeader> getEntitiesByType(List<AtlasEntityHeader> entitiesByOperation, String typeName) {
        List<AtlasEntityHeader> ret = new ArrayList<>();

        if ( entitiesByOperation != null && entitiesByOperation.size() > 0) {
            for (AtlasEntityHeader header : entitiesByOperation) {
                if ( header.getTypeName().equals(typeName)) {
                    ret.add(header);
                }
            }
        }
        return ret;
    }

    private AtlasEntityHeader getEntityByTypeAndUniqueAttribute(List<AtlasEntityHeader> entitiesByOperation, String typeName, String attrName, String attrVal) {
        if (entitiesByOperation != null && entitiesByOperation.size() > 0) {
            for (AtlasEntityHeader header : entitiesByOperation) {
                if (header.getTypeName().equals(typeName)) {
                    if (attrVal != null && attrVal.equals(header.getAttribute(attrName))) {
                        return header;
                    }
                }
            }
        }
        return null;
    }
}
