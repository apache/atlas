/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.model.instance;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.atlas.model.instance.EntityMutations.EntityOperation;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.commons.collections.CollectionUtils;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class EntityMutationResponse implements Serializable {
    private static final long serialVersionUID = 1L;

    private Map<EntityOperation, List<AtlasEntityHeader>> mutatedEntities;
    @JsonIgnore
    private transient Map<EntityOperation, Set<String>>   entityGuidsPerOp;
    private Map<String, String>                           guidAssignments;
    private List<FailedEntity>                            failedEntities;
    private MutationSummary                               summary;

    public EntityMutationResponse() {
    }

    public EntityMutationResponse(final Map<EntityOperation, List<AtlasEntityHeader>> mutatedEntities) {
        this.mutatedEntities = mutatedEntities;
        rebuildEntityGuidsPerOp();
    }

    public Map<EntityOperation, List<AtlasEntityHeader>> getMutatedEntities() {
        return mutatedEntities;
    }

    public void setMutatedEntities(final Map<EntityOperation, List<AtlasEntityHeader>> mutatedEntities) {
        this.mutatedEntities = mutatedEntities;
        rebuildEntityGuidsPerOp();
    }

    public Map<String, String> getGuidAssignments() {
        return guidAssignments;
    }

    public void setGuidAssignments(Map<String, String> guidAssignments) {
        this.guidAssignments = guidAssignments;
    }

    public List<FailedEntity> getFailedEntities() {
        return failedEntities;
    }

    public void setFailedEntities(List<FailedEntity> failedEntities) {
        this.failedEntities = failedEntities;
    }

    public MutationSummary getSummary() {
        return summary;
    }

    @JsonDeserialize(as = PurgeSummary.class)
    public void setSummary(MutationSummary summary) {
        this.summary = summary;
    }

    @JsonIgnore
    public PurgeSummary getPurgeSummary() {
        return summary instanceof PurgeSummary ? (PurgeSummary) summary : null;
    }

    public void addFailedEntity(FailedEntity failedEntity) {
        if (failedEntity == null) {
            return;
        }

        if (failedEntities == null) {
            failedEntities = new ArrayList<>();
        }

        failedEntities.add(failedEntity);
    }

    @JsonIgnore
    public List<AtlasEntityHeader> getEntitiesByOperation(EntityOperation op) {
        if (mutatedEntities != null) {
            return mutatedEntities.get(op);
        }

        return null;
    }

    @JsonIgnore
    public List<AtlasEntityHeader> getCreatedEntities() {
        if (mutatedEntities != null) {
            return mutatedEntities.get(EntityOperation.CREATE);
        }

        return null;
    }

    @JsonIgnore
    public List<AtlasEntityHeader> getUpdatedEntities() {
        if (mutatedEntities != null) {
            return mutatedEntities.get(EntityOperation.UPDATE);
        }

        return null;
    }

    public List<AtlasEntityHeader> getPartialUpdatedEntities() {
        if (mutatedEntities != null) {
            return mutatedEntities.get(EntityOperation.PARTIAL_UPDATE);
        }

        return null;
    }

    @JsonIgnore
    public List<AtlasEntityHeader> getDeletedEntities() {
        if (mutatedEntities != null) {
            return mutatedEntities.get(EntityOperation.DELETE);
        }

        return null;
    }

    @JsonIgnore
    public List<AtlasEntityHeader> getPurgedEntities() {
        if (mutatedEntities != null) {
            return mutatedEntities.get(EntityOperation.PURGE);
        }

        return null;
    }

    @JsonIgnore
    public String getPurgedEntitiesIds() {
        String                  ret            = null;
        List<AtlasEntityHeader> purgedEntities = getPurgedEntities();

        if (CollectionUtils.isNotEmpty(purgedEntities)) {
            List<String> entityIds = purgedEntities.stream().map(AtlasEntityHeader::getGuid).collect(Collectors.toList());

            ret = String.join(",", entityIds);
        }

        return ret;
    }

    @JsonIgnore
    public AtlasEntityHeader getFirstEntityCreated() {
        final List<AtlasEntityHeader> entitiesByOperation = getEntitiesByOperation(EntityOperation.CREATE);

        if (entitiesByOperation != null && !entitiesByOperation.isEmpty()) {
            return entitiesByOperation.get(0);
        }

        return null;
    }

    @JsonIgnore
    public AtlasEntityHeader getFirstEntityUpdated() {
        final List<AtlasEntityHeader> entitiesByOperation = getEntitiesByOperation(EntityOperation.UPDATE);

        if (entitiesByOperation != null && !entitiesByOperation.isEmpty()) {
            return entitiesByOperation.get(0);
        }

        return null;
    }

    @JsonIgnore
    public AtlasEntityHeader getFirstEntityPartialUpdated() {
        final List<AtlasEntityHeader> entitiesByOperation = getEntitiesByOperation(EntityOperation.PARTIAL_UPDATE);

        if (entitiesByOperation != null && !entitiesByOperation.isEmpty()) {
            return entitiesByOperation.get(0);
        }

        return null;
    }

    @JsonIgnore
    public AtlasEntityHeader getFirstCreatedEntityByTypeName(String typeName) {
        return getFirstEntityByType(getEntitiesByOperation(EntityOperation.CREATE), typeName);
    }

    @JsonIgnore
    public AtlasEntityHeader getFirstDeletedEntityByTypeName(String typeName) {
        return getFirstEntityByType(getEntitiesByOperation(EntityOperation.DELETE), typeName);
    }

    @JsonIgnore
    public List<AtlasEntityHeader> getCreatedEntitiesByTypeName(String typeName) {
        return getEntitiesByType(getEntitiesByOperation(EntityOperation.CREATE), typeName);
    }

    @JsonIgnore
    public List<AtlasEntityHeader> getPartialUpdatedEntitiesByTypeName(String typeName) {
        return getEntitiesByType(getEntitiesByOperation(EntityOperation.PARTIAL_UPDATE), typeName);
    }

    @JsonIgnore
    public AtlasEntityHeader getCreatedEntityByTypeNameAndAttribute(String typeName, String attrName, String attrVal) {
        return getEntityByTypeAndUniqueAttribute(getEntitiesByOperation(EntityOperation.CREATE), typeName, attrName, attrVal);
    }

    @JsonIgnore

    public AtlasEntityHeader getUpdatedEntityByTypeNameAndAttribute(String typeName, String attrName, String attrVal) {
        return getEntityByTypeAndUniqueAttribute(getEntitiesByOperation(EntityOperation.UPDATE), typeName, attrName, attrVal);
    }

    @JsonIgnore
    public List<AtlasEntityHeader> getUpdatedEntitiesByTypeName(String typeName) {
        return getEntitiesByType(getEntitiesByOperation(EntityOperation.UPDATE), typeName);
    }

    @JsonIgnore
    public List<AtlasEntityHeader> getDeletedEntitiesByTypeName(String typeName) {
        return getEntitiesByType(getEntitiesByOperation(EntityOperation.DELETE), typeName);
    }

    @JsonIgnore
    public AtlasEntityHeader getFirstUpdatedEntityByTypeName(String typeName) {
        return getFirstEntityByType(getEntitiesByOperation(EntityOperation.UPDATE), typeName);
    }

    @JsonIgnore
    public AtlasEntityHeader getFirstPartialUpdatedEntityByTypeName(String typeName) {
        return getFirstEntityByType(getEntitiesByOperation(EntityOperation.PARTIAL_UPDATE), typeName);
    }

    @JsonIgnore
    public void addEntity(EntityOperation op, AtlasEntityHeader header) {
        if (header == null) {
            return;
        }

        // Duplicate GUID under CREATE: keep the first header, ignore later additions
        String guid = header.getGuid();
        if (op == EntityOperation.UPDATE || op == EntityOperation.PARTIAL_UPDATE) {
            if (entityHeaderExists(EntityOperation.CREATE, guid)) {
                op = EntityOperation.CREATE;
            }
        }

        if (mutatedEntities == null) {
            mutatedEntities = new HashMap<>();
        }

        List<AtlasEntityHeader> opEntities = mutatedEntities.computeIfAbsent(op, k -> new ArrayList<>());

        if (guid == null) {
            opEntities.add(header);
            return;
        }

        if (getGuidsForOperation(op).add(guid)) {
            opEntities.add(header);
        }
    }

    public StringBuilder toString(StringBuilder sb) {
        if (sb == null) {
            sb = new StringBuilder();
        }

        AtlasBaseTypeDef.dumpObjects(mutatedEntities, sb);

        return sb;
    }

    @Override
    public int hashCode() {
        return Objects.hash(mutatedEntities, guidAssignments, failedEntities, summary);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EntityMutationResponse that = (EntityMutationResponse) o;

        return Objects.equals(mutatedEntities, that.mutatedEntities) &&
                Objects.equals(guidAssignments, that.guidAssignments) &&
                Objects.equals(failedEntities, that.failedEntities) &&
                Objects.equals(summary, that.summary);
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }

    private Set<String> getGuidsForOperation(EntityOperation op) {
        if (entityGuidsPerOp == null) {
            rebuildEntityGuidsPerOp();
        }

        return entityGuidsPerOp.computeIfAbsent(op, k -> new HashSet<>());
    }

    private void rebuildEntityGuidsPerOp() {
        entityGuidsPerOp = new HashMap<>();

        if (mutatedEntities == null) {
            return;
        }

        for (Map.Entry<EntityOperation, List<AtlasEntityHeader>> entry : mutatedEntities.entrySet()) {
            Set<String> guids = new HashSet<>();
            List<AtlasEntityHeader> headers = entry.getValue();

            if (headers != null) {
                for (AtlasEntityHeader header : headers) {
                    if (header.getGuid() != null) {
                        guids.add(header.getGuid());
                    }
                }
            }

            entityGuidsPerOp.put(entry.getKey(), guids);
        }
    }

    private boolean entityHeaderExists(EntityOperation op, String guid) {
        return guid != null && getGuidsForOperation(op).contains(guid);
    }

    private AtlasEntityHeader getFirstEntityByType(List<AtlasEntityHeader> entitiesByOperation, String typeName) {
        if (entitiesByOperation != null && !entitiesByOperation.isEmpty()) {
            for (AtlasEntityHeader header : entitiesByOperation) {
                if (header.getTypeName().equals(typeName)) {
                    return header;
                }
            }
        }

        return null;
    }

    private List<AtlasEntityHeader> getEntitiesByType(List<AtlasEntityHeader> entitiesByOperation, String typeName) {
        List<AtlasEntityHeader> ret = new ArrayList<>();

        if (entitiesByOperation != null && !entitiesByOperation.isEmpty()) {
            for (AtlasEntityHeader header : entitiesByOperation) {
                if (header.getTypeName().equals(typeName)) {
                    ret.add(header);
                }
            }
        }

        return ret;
    }

    private AtlasEntityHeader getEntityByTypeAndUniqueAttribute(List<AtlasEntityHeader> entitiesByOperation, String typeName, String attrName, String attrVal) {
        if (entitiesByOperation != null && !entitiesByOperation.isEmpty()) {
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
