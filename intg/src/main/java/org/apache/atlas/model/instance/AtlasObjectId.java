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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.atlas.model.PList;
import org.apache.atlas.model.SearchFilter.SortType;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.commons.lang.StringUtils;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * Reference to an object-instance of an Atlas type - like entity.
 */
@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class AtlasObjectId implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final String KEY_GUID              = "guid";
    public static final String KEY_TYPENAME          = "typeName";
    public static final String KEY_UNIQUE_ATTRIBUTES = "uniqueAttributes";

    private String              guid;
    private String              typeName;
    private Map<String, Object> uniqueAttributes;

    public AtlasObjectId() {
        this(null, null, null);
    }

    public AtlasObjectId(String guid) {
        this(guid, null, null);
    }

    public AtlasObjectId(String guid, String typeName) {
        this(guid, typeName, null);
    }

    public AtlasObjectId(String typeName, Map<String, Object> uniqueAttributes) {
        this(null, typeName, uniqueAttributes);
    }

    public AtlasObjectId(String typeName, final String attrName, final Object attrValue) {
        this(null, typeName, createMap(attrName, attrValue));
    }

    public AtlasObjectId(String guid, String typeName, Map<String, Object> uniqueAttributes) {
        setGuid(guid);
        setTypeName(typeName);
        setUniqueAttributes(uniqueAttributes);
    }

    public AtlasObjectId(AtlasObjectId other) {
        if (other != null) {
            setGuid(other.getGuid());
            setTypeName(other.getTypeName());
            setUniqueAttributes(other.getUniqueAttributes());
        }
    }

    public AtlasObjectId(Map objIdMap) {
        if (objIdMap != null) {
            Object g = objIdMap.get(KEY_GUID);
            Object t = objIdMap.get(KEY_TYPENAME);
            Object u = objIdMap.get(KEY_UNIQUE_ATTRIBUTES);

            if (g != null) {
                setGuid(g.toString());
            }

            if (t != null) {
                setTypeName(t.toString());
            }

            if (u instanceof Map) {
                setUniqueAttributes((Map) u);
            }
        }
    }

    public String getGuid() {
        return guid;
    }

    public void setGuid(String guid) {
        this.guid = guid;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public Map<String, Object> getUniqueAttributes() {
        return uniqueAttributes;
    }

    public void setUniqueAttributes(Map<String, Object> uniqueAttributes) {
        this.uniqueAttributes = uniqueAttributes;
    }

    public StringBuilder toString(StringBuilder sb) {
        if (sb == null) {
            sb = new StringBuilder();
        }

        sb.append("AtlasObjectId{");
        sb.append("guid='").append(guid).append('\'');
        sb.append(", typeName='").append(typeName).append('\'');
        sb.append(", uniqueAttributes={");
        AtlasBaseTypeDef.dumpObjects(uniqueAttributes, sb);
        sb.append('}');
        sb.append('}');

        return sb;
    }

    @Override
    public int hashCode() {
        return guid != null ? Objects.hash(guid) : Objects.hash(typeName, uniqueAttributes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AtlasObjectId that = (AtlasObjectId) o;

        // if guid is empty/null, equality should be based on typeName/uniqueAttributes
        if (StringUtils.isEmpty(guid) && StringUtils.isEmpty(that.guid)) {
            return Objects.equals(typeName, that.typeName) && Objects.equals(uniqueAttributes, that.uniqueAttributes);
        } else {
            return Objects.equals(guid, that.guid);
        }
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }

    private static Map<String, Object> createMap(String key, Object val) {
        Map<String, Object> ret = new HashMap<>();

        ret.put(key, val);

        return ret;
    }

    /**
     * REST serialization friendly list.
     */
    @JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
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

        public AtlasObjectIds(List<AtlasObjectId> list, long startIndex, int pageSize, long totalCount, SortType sortType, String sortBy) {
            super(list, startIndex, pageSize, totalCount, sortType, sortBy);
        }
    }
}
