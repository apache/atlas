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


import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.NONE;
import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class EntityMutationResponse {

    Map<EntityMutations.EntityOperation, List<AtlasEntityHeader>> entitiesMutated = new HashMap<>();

    public EntityMutationResponse() {
    }

    public EntityMutationResponse(final Map<EntityMutations.EntityOperation, List<AtlasEntityHeader>> opVsEntityMap) {
        this.entitiesMutated = opVsEntityMap;
    }

    public Map<EntityMutations.EntityOperation, List<AtlasEntityHeader>> getEntitiesMutated() {
        return entitiesMutated;
    }

    public void setEntitiesMutated(final Map<EntityMutations.EntityOperation, List<AtlasEntityHeader>> opVsEntityMap) {
        this.entitiesMutated = opVsEntityMap;
    }

    public List<AtlasEntityHeader> getEntitiesByOperation(EntityMutations.EntityOperation op) {
        if ( entitiesMutated != null) {
            return entitiesMutated.get(op);
        }
        return null;
    }

    public void addEntity(EntityMutations.EntityOperation op, AtlasEntityHeader header) {
        if (entitiesMutated == null) {
            entitiesMutated = new HashMap<>();
        }

        if (entitiesMutated != null && entitiesMutated.get(op) == null) {
            entitiesMutated.put(op, new ArrayList<AtlasEntityHeader>());
        }
        entitiesMutated.get(op).add(header);
    }


    public StringBuilder toString(StringBuilder sb) {
        if ( sb == null) {
            sb = new StringBuilder();
        }

        if (MapUtils.isNotEmpty(entitiesMutated)) {
            int i = 0;
            for (Map.Entry<EntityMutations.EntityOperation, List<AtlasEntityHeader>> e : entitiesMutated.entrySet()) {
                if (i > 0) {
                    sb.append(",");
                }
                sb.append(e.getKey()).append(":");
                if (CollectionUtils.isNotEmpty(e.getValue())) {
                    for (int j = 0; i < e.getValue().size(); j++) {
                        if (j > 0) {
                            sb.append(",");
                        }
                        e.getValue().get(i).toString(sb);
                    }
                }
                i++;
            }
        }

        return sb;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EntityMutationResponse that = (EntityMutationResponse) o;
        return Objects.equals(entitiesMutated, that.entitiesMutated);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entitiesMutated);
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }
}
