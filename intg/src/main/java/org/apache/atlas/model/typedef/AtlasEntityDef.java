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
package org.apache.atlas.model.typedef;

import org.apache.atlas.model.PList;
import org.apache.atlas.model.SearchFilter.SortType;
import org.apache.atlas.model.TypeCategory;
import org.apache.commons.collections.CollectionUtils;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.NONE;
import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.PUBLIC_ONLY;


/**
 * class that captures details of a entity-type.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class AtlasEntityDef extends AtlasStructDef implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private Set<String> superTypes;

    // subTypes field below is derived from 'superTypes' specified in all AtlasEntityDef
    // this value is ignored during create & update operations
    private Set<String> subTypes;


    public AtlasEntityDef() {
        this(null, null, null, null, null, null);
    }

    public AtlasEntityDef(String name) {
        this(name, null, null, null, null, null);
    }

    public AtlasEntityDef(String name, String description) {
        this(name, description, null, null, null, null);
    }

    public AtlasEntityDef(String name, String description, String typeVersion) {
        this(name, description, typeVersion, null, null, null);
    }

    public AtlasEntityDef(String name, String description, String typeVersion, List<AtlasAttributeDef> attributeDefs) {
        this(name, description, typeVersion, attributeDefs, null, null);
    }

    public AtlasEntityDef(String name, String description, String typeVersion, List<AtlasAttributeDef> attributeDefs,
                          Set<String> superTypes) {
        this(name, description, typeVersion, attributeDefs, superTypes, null);
    }

    public AtlasEntityDef(String name, String description, String typeVersion, List<AtlasAttributeDef> attributeDefs,
                          Set<String> superTypes, Map<String, String> options) {
        super(TypeCategory.ENTITY, name, description, typeVersion, attributeDefs, options);

        setSuperTypes(superTypes);
    }

    public AtlasEntityDef(AtlasEntityDef other) {
        super(other);

        setSuperTypes(other != null ? other.getSuperTypes() : null);
    }

    public Set<String> getSuperTypes() {
        return superTypes;
    }

    public void setSuperTypes(Set<String> superTypes) {
        if (superTypes != null && this.superTypes == superTypes) {
            return;
        }

        if (CollectionUtils.isEmpty(superTypes)) {
            this.superTypes = new HashSet<>();
        } else {
            this.superTypes = new HashSet<>(superTypes);
        }
    }

    public Set<String> getSubTypes() {
        return subTypes;
    }

    public void setSubTypes(Set<String> subTypes) {
        this.subTypes = subTypes;
    }

    public boolean hasSuperType(String typeName) {
        return hasSuperType(superTypes, typeName);
    }

    public void addSuperType(String typeName) {
        Set<String> s = this.superTypes;

        if (!hasSuperType(s, typeName)) {
            s = new HashSet<>(s);

            s.add(typeName);

            this.superTypes = s;
        }
    }

    public void removeSuperType(String typeName) {
        Set<String> s = this.superTypes;

        if (hasSuperType(s, typeName)) {
            s = new HashSet<>(s);

            s.remove(typeName);

            this.superTypes = s;
        }
    }

    private static boolean hasSuperType(Set<String> superTypes, String typeName) {
        return superTypes != null && typeName != null && superTypes.contains(typeName);
    }

    @Override
    public StringBuilder toString(StringBuilder sb) {
        if (sb == null) {
            sb = new StringBuilder();
        }

        sb.append("AtlasEntityDef{");
        super.toString(sb);
        sb.append(", superTypes=[");
        dumpObjects(superTypes, sb);
        sb.append("]");
        sb.append('}');

        return sb;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        if (!super.equals(o)) { return false; }

        AtlasEntityDef that = (AtlasEntityDef) o;
        return Objects.equals(superTypes, that.superTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), superTypes);
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
    @XmlSeeAlso(AtlasEntityDef.class)
    public static class AtlasEntityDefs extends PList<AtlasEntityDef> {
        private static final long serialVersionUID = 1L;

        public AtlasEntityDefs() {
            super();
        }

        public AtlasEntityDefs(List<AtlasEntityDef> list) {
            super(list);
        }

        public AtlasEntityDefs(List list, long startIndex, int pageSize, long totalCount,
                               SortType sortType, String sortBy) {
            super(list, startIndex, pageSize, totalCount, sortType, sortBy);
        }
    }
}
