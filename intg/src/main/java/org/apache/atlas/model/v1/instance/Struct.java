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

package org.apache.atlas.model.v1.instance;


import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.NONE;
import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.PUBLIC_ONLY;


@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class Struct implements Serializable {
    private static final long serialVersionUID = 1L;

    private String              typeName;
    private Map<String, Object> values;


    public Struct() {
    }

    public Struct(Struct that) {
        if (that != null) {
            this.typeName = that.typeName;

            if (that.values != null) {
                this.values = new HashMap<>(that.values);
            }
        }
    }

    public Struct(String typeName) {
        this(typeName, null);
    }

    public Struct(String typeName, Map<String, Object> values) {
        this.typeName = typeName;
        this.values   = values;
    }

    // for serialization backward compatibility
    public String getJsonClass() {
        return "org.apache.atlas.typesystem.json.InstanceSerialization$_Struct";
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public Map<String, Object> getValues() {
        return values;
    }

    public void setValues(Map<String, Object> values) {
        this.values = values;
    }

    @JsonIgnore
    public Map<String, Object> getValuesMap() {
        return values;
    }

    @JsonIgnore
    public void set(String attrName, Object attrValue) {
        if (values == null) {
            values = new HashMap<>();
        }

        values.put(attrName, attrValue);
    }

    @JsonIgnore
    public Object get(String attrName) {
        return values != null ? values.get(attrName) : null;
    }

    @JsonIgnore
    public void setNull(String attrName) {
        if (values != null) {
            values.remove(attrName);
        }
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || o.getClass() != getClass()) {
            return false;
        }

        Struct obj = (Struct)o;

        return Objects.equals(typeName, obj.typeName) &&
               Objects.equals(values, obj.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeName, values);
    }
}
