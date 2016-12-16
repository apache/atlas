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

import org.apache.atlas.model.PList;
import org.apache.atlas.model.SearchFilter.SortType;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.NONE;
import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.PUBLIC_ONLY;


/**
 * An instance of an entity - like hive_table, hive_database along with its assictaed classifications, terms etc.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class AtlasEntityWithAssociations extends AtlasEntity implements Serializable {
    private static final long serialVersionUID = 1L;

    List<AtlasClassification> classifications;

    public AtlasEntityWithAssociations() {
        this(null, null);
    }

    public AtlasEntityWithAssociations(String typeName) {
        this(typeName, null);
    }

    public AtlasEntityWithAssociations(AtlasEntityDef entityDef) {
        this(entityDef != null ? entityDef.getName() : null, null);
    }

    public AtlasEntityWithAssociations(String typeName, Map<String, Object> attributes) {
        super(typeName, attributes);
        setClassifications(null);
    }

    public AtlasEntityWithAssociations(AtlasEntityWithAssociations other) {
        super(other);

        setClassifications(other != null ? other.getClassifications() : null);
    }

    @Override
    public StringBuilder toString(StringBuilder sb) {
        if (sb == null) {
            sb = new StringBuilder();
        }

        sb.append("AtlasEntityWithAssociations{");
        sb.append("classifications='").append(classifications).append('\'');
        sb.append(", ");
        super.toString(sb);
        sb.append('}');

        return sb;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        AtlasEntityWithAssociations that = (AtlasEntityWithAssociations) o;
        return Objects.equals(classifications, that.classifications);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), classifications);
    }

    public List<AtlasClassification> getClassifications() {
        return classifications;
    }

    public void setClassifications(final List<AtlasClassification> classifications) {
        this.classifications = classifications;
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
    @XmlSeeAlso(AtlasEntityWithAssociations.class)
    public static class AtlasEntitiesWithAssociations extends PList<AtlasEntityWithAssociations> {
        private static final long serialVersionUID = 1L;

        public AtlasEntitiesWithAssociations() {
            super();
        }

        public AtlasEntitiesWithAssociations(List<AtlasEntityWithAssociations> list) {
            super(list);
        }

        public AtlasEntitiesWithAssociations(List list, long startIndex, int pageSize, long totalCount,
                             SortType sortType, String sortBy) {
            super(list, startIndex, pageSize, totalCount, sortType, sortBy);
        }
    }
}
