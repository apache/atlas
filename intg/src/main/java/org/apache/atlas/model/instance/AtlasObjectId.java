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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;

import org.apache.atlas.model.PList;
import org.apache.atlas.model.SearchFilter.SortType;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.PUBLIC_ONLY;
import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.NONE;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * Reference to an object-instance of an Atlas type - like entity.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class AtlasObjectId  implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final String KEY_TYPENAME          = "typeName";
    public static final String KEY_GUID              = "guid";
    public static final String KEY_UNIQUE_ATTRIBUTES = "uniqueAttributes";

    private String              typeName;
    private String              guid;
    private Map<String, Object> uniqueAttributes;

    @JsonIgnore
    private boolean isAssignedGuid = false;

    @JsonIgnore
    private boolean isUnAssignedGuid = false;

    public AtlasObjectId() {
        this(null, null, null);
    }

    public AtlasObjectId(String typeName) {
        this(typeName, null, null);
    }

    public AtlasObjectId(String typeName, String guid) {
        this(typeName, guid, null);
    }

    public AtlasObjectId(String typeName, Map<String, Object> uniqueAttributes) {
        this(typeName, null, uniqueAttributes);
    }

    public AtlasObjectId(String typeName, String guid, Map<String, Object> uniqueAttributes) {
        setTypeName(typeName);
        setGuid(guid);
        setUniqueAttributes(uniqueAttributes);
    }

    public AtlasObjectId(AtlasObjectId other) {
        if (other != null) {
            setTypeName(other.getTypeName());
            setGuid(other.getGuid());
            setUniqueAttributes(other.getUniqueAttributes());
        }
    }

    public AtlasObjectId(Map objIdMap) {
        if (objIdMap != null) {
            Object t = objIdMap.get(KEY_TYPENAME);
            Object g = objIdMap.get(KEY_GUID);
            Object u = objIdMap.get(KEY_UNIQUE_ATTRIBUTES);

            if (t != null) {
                setTypeName(t.toString());
            }

            if (g != null) {
                setGuid(g.toString());
            }

            if (u != null && u instanceof Map) {
                setUniqueAttributes((Map)u);
            }
        }
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public String getGuid() {
        return guid;
    }

    public void setGuid(String guid) {
        this.guid = guid;
        if ( guid != null) {
            this.isAssignedGuid = AtlasEntity.isAssigned(guid);
            this.isUnAssignedGuid = AtlasEntity.isUnAssigned(guid);
        }
    }

    public Map<String, Object> getUniqueAttributes() {
        return uniqueAttributes;
    }

    public void setUniqueAttributes(Map<String, Object> uniqueAttributes) {
        this.uniqueAttributes = uniqueAttributes;
    }

    @JsonIgnore
    public boolean isAssignedGuid() {
        return isAssignedGuid;
    }

    @JsonIgnore
    public boolean isUnAssignedGuid() {
        return isUnAssignedGuid;
    }

    @JsonIgnore
    public boolean isValid() {
        if (StringUtils.isEmpty(typeName)) {
            return false;
        } else if (StringUtils.isNotEmpty(guid)) {
            if (!isAssignedGuid && !isUnAssignedGuid) {
                return false;
            }
        } else if (MapUtils.isEmpty(uniqueAttributes)) {
            return false;
        }

        return true;
    }

    public StringBuilder toString(StringBuilder sb) {
        if (sb == null) {
            sb = new StringBuilder();
        }

        sb.append("AtlasObjectId{");
        sb.append("typeName='").append(typeName).append('\'');
        sb.append(", guid='").append(guid).append('\'');
        sb.append(", uniqueAttributes={");
        AtlasBaseTypeDef.dumpObjects(uniqueAttributes, sb);
        sb.append('}');
        sb.append('}');

        return sb;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AtlasObjectId that = (AtlasObjectId) o;
        return Objects.equals(typeName, that.typeName) &&
                Objects.equals(guid, that.guid) &&
                Objects.equals(uniqueAttributes, that.uniqueAttributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeName, guid, uniqueAttributes);
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }

    /**
     * REST serialization friendly list.
     */
    @JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown=true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.PROPERTY)
    @XmlSeeAlso(AtlasObjectId.class)
    public static class AtlasObjectIds extends PList<AtlasObjectId> {
        private static final long serialVersionUID = 1L;

        public AtlasObjectIds() {
            super();
        }

        public AtlasObjectIds(List<AtlasObjectId> list) {
            super(list);
        }

        public AtlasObjectIds(List list, long startIndex, int pageSize, long totalCount,
                              SortType sortType, String sortBy) {
            super(list, startIndex, pageSize, totalCount, sortType, sortBy);
        }
    }
}
