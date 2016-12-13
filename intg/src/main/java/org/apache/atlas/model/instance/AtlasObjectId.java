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
import org.codehaus.jackson.annotate.JsonAutoDetect;
import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.PUBLIC_ONLY;
import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.NONE;

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

    public static final String KEY_TYPENAME = "typeName";
    public static final String KEY_GUID     = "guid";

    private String typeName;
    private String guid;

    public AtlasObjectId() {
        this(null, null);
    }

    public AtlasObjectId(String typeName) {
        this(typeName, null);
    }

    public AtlasObjectId(String typeName, String guid) {
        setTypeName(typeName);
        setGuid(guid);
    }

    public AtlasObjectId(AtlasObjectId other) {
        if (other != null) {
            setTypeName(other.getTypeName());
            setGuid(other.getGuid());
        }
    }

    public AtlasObjectId(Map objIdMap) {
        if (objIdMap != null) {
            Object t = objIdMap.get(KEY_TYPENAME);
            Object g = objIdMap.get(KEY_GUID);

            if (t != null) {
                setTypeName(t.toString());
            }

            if (g != null) {
                setGuid(g.toString());
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
    }

    public StringBuilder toString(StringBuilder sb) {
        if (sb == null) {
            sb = new StringBuilder();
        }

        sb.append("AtlasObjectId{");
        sb.append("typeName='").append(typeName).append('\'');
        sb.append(", guid='").append(guid).append('\'');
        sb.append('}');

        return sb;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AtlasObjectId that = (AtlasObjectId) o;
        return Objects.equals(typeName, that.typeName) &&
                Objects.equals(guid, that.guid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeName, guid);
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
