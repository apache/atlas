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
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;


/**
 * An extension of an AtlasEntityHeader with relationshipAttributes
 */
@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class AtlasEntityHeaderWithRelations extends AtlasEntityHeader implements Serializable {
    private static final long serialVersionUID = 1L;

    private Map<String, Object> relationshipAttributes = null;
    private Map<String, Object> internalAttributes = null;

    public AtlasEntityHeaderWithRelations(String typeName, String guid, Map<String, Object> attributes) {
        super(typeName, attributes);
        setGuid(guid);
        setClassificationNames(null);
        setClassifications(null);
        setLabels(null);
    }

    public Map<String, Object> getRelationshipAttributes() {
        return relationshipAttributes;
    }

    public void setRelationshipAttributes(Map<String, Object> relationshipAttributes) {
        this.relationshipAttributes = relationshipAttributes;
    }

    public Map<String, Object> getInternalAttributes() {
        return internalAttributes;
    }

    public void setInternalAttributes(Map<String, Object> internalAttributes) {
        this.internalAttributes = internalAttributes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        AtlasEntityHeaderWithRelations that = (AtlasEntityHeaderWithRelations) o;
        return Objects.equals(relationshipAttributes, that.relationshipAttributes) &&
                Objects.equals(internalAttributes, that.internalAttributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), relationshipAttributes, internalAttributes);
    }

    @Override
    public String toString() {
        return "AtlasEntityHeaderWithRelations{" +
                "relationshipAttributes=" + relationshipAttributes +
                "internalAttributes=" + internalAttributes +
                "AtlasEntityHeader=" + super.toString() +
                '}';
    }
}
