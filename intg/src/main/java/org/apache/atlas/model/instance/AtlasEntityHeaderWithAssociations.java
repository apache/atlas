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
import org.apache.atlas.model.SearchFilter;
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
 * An instance of an entity and its associations - like hive_table, hive_database.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class AtlasEntityHeaderWithAssociations extends AtlasEntityHeader implements Serializable{
    private static final long serialVersionUID = 1L;

    private List<String> classificationNames;

    public AtlasEntityHeaderWithAssociations(){
        this(null, null);
    }

    public AtlasEntityHeaderWithAssociations(AtlasEntityDef entityDef) {
        this(entityDef != null ? entityDef.getName() : null, null);
    }

    public AtlasEntityHeaderWithAssociations(String typeName, Map<String, Object> attributes) {
        super(typeName, attributes);
        setClassificationNames(null);
    }

    public AtlasEntityHeaderWithAssociations(AtlasEntityHeaderWithAssociations other) {
        super(other);

        if (other != null) {
            setClassificationNames(other.getClassificationNames());
        }
    }

    public List<String> getClassificationNames(){
        return classificationNames;
    }

    public void setClassificationNames(List<String> classificationNames) {
        this.classificationNames = classificationNames;
    }


    @Override
    public StringBuilder toString(StringBuilder sb) {
        if (sb == null) {
            sb = new StringBuilder();
        }

        sb.append("AtlasEntityHeaderwithAssociations{");
        sb.append(", classificationNames=[");
        dumpObjects(classificationNames, sb);
        sb.append("],");
        super.toString(sb);
        sb.append('}');

        return sb;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        AtlasEntityHeaderWithAssociations that = (AtlasEntityHeaderWithAssociations) o;
        return Objects.equals(classificationNames, that.classificationNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), classificationNames);
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
    @XmlSeeAlso(AtlasEntity.class)
    public static class  AtlasEntityHeadersWithAssociations extends PList<AtlasEntityHeaderWithAssociations> {
        private static final long serialVersionUID = 1L;

        public AtlasEntityHeadersWithAssociations() {
            super();
        }

        public AtlasEntityHeadersWithAssociations(List<AtlasEntityHeaderWithAssociations> list) {
            super(list);
        }

        public AtlasEntityHeadersWithAssociations(List list, long startIndex, int pageSize, long totalCount,
                                  SearchFilter.SortType sortType, String sortBy) {
            super(list, startIndex, pageSize, totalCount, sortType, sortBy);
        }
    }

}
