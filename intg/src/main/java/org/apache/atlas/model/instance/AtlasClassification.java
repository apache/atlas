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


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

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
import org.apache.atlas.model.TimeBoundary;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;


/**
 * An instance of a classification; it doesn't have an identity, this object exists only when associated with an entity.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class AtlasClassification extends AtlasStruct implements Serializable {
    private static final long serialVersionUID = 1L;

    private String       entityGuid     = null;
    private boolean      propagate      = true;
    private TimeBoundary validityPeriod = null;


    public AtlasClassification() {
        this(null, null);
    }

    public AtlasClassification(String typeName) {
        this(typeName, null);
    }

    public AtlasClassification(String typeName, Map<String, Object> attributes) {
        super(typeName, attributes);
    }

    public AtlasClassification(String typeName, String attrName, Object attrValue) {
        super(typeName, attrName, attrValue);
    }

    public AtlasClassification(Map map) {
        super(map);
    }

    public AtlasClassification(AtlasClassification other) {
        if (other != null) {
            setTypeName(other.getTypeName());
            setAttributes(other.getAttributes());
        }
    }

    public String getEntityGuid() {
        return entityGuid;
    }

    public void setEntityGuid(String entityGuid) {
        this.entityGuid = entityGuid;
    }

    public boolean isPropagate() {
        return propagate;
    }

    public void setPropagate(boolean propagate) {
        this.propagate = propagate;
    }

    public TimeBoundary getValidityPeriod() {
        return validityPeriod;
    }

    public void setValidityPeriod(TimeBoundary validityPeriod) {
        this.validityPeriod = validityPeriod;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        if (!super.equals(o)) { return false; }
        AtlasClassification that = (AtlasClassification) o;
        return propagate == that.propagate &&
               Objects.equals(entityGuid, that.entityGuid) &&
               Objects.equals(validityPeriod, that.validityPeriod);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), entityGuid, propagate);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AtlasClassification{");
        super.toString(sb);
        sb.append("entityGuid='").append(entityGuid).append('\'');
        sb.append(", propagate=").append(propagate);
        sb.append(", validityPeriod=").append(validityPeriod);
        sb.append('}');
        return sb.toString();
    }

    /**
     * REST serialization friendly list.
     */
    @JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown=true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.PROPERTY)
    @XmlSeeAlso(AtlasClassification.class)
    public static class AtlasClassifications extends PList<AtlasClassification> {
        private static final long serialVersionUID = 1L;

        public AtlasClassifications() {
            super();
        }

        public AtlasClassifications(List<AtlasClassification> list) {
            super(list);
        }

        public AtlasClassifications(List list, long startIndex, int pageSize, long totalCount,
                                    SortType sortType, String sortBy) {
            super(list, startIndex, pageSize, totalCount, sortType, sortBy);
        }
    }
}
