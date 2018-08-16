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

package org.apache.atlas.typesystem.types;

import org.apache.atlas.AtlasException;
import org.apache.atlas.type.AtlasType;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class AttributeInfo {
    public final String name;
    public final Multiplicity multiplicity;
    //A composite is the one whose lifecycle is dependent on the enclosing type and is not just a reference
    public final boolean isComposite;
    public final boolean isUnique;
    public final boolean isIndexable;
    /**
     * If this is a reference attribute, then the name of the attribute on the Class
     * that this refers to.
     */
    public final String reverseAttributeName;
    public final boolean isSoftRef;
    private IDataType dataType;

    public AttributeInfo(TypeSystem t, AttributeDefinition def, Map<String, IDataType> tempTypes) throws AtlasException {
        this.name = def.name;
        this.dataType =
                (tempTypes != null && tempTypes.containsKey(def.dataTypeName)) ? tempTypes.get(def.dataTypeName) :
                        t.getDataType(IDataType.class, def.dataTypeName);
        this.multiplicity = def.multiplicity;
        this.isComposite = def.isComposite;
        this.isUnique = def.isUnique;
        this.isIndexable = def.isIndexable;
        this.reverseAttributeName = def.reverseAttributeName;
        this.isSoftRef = def.isSoftRef;
    }

    public IDataType dataType() {
        return dataType;
    }

    void setDataType(IDataType dT) {
        dataType = dT;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        try {
            output(buf, new HashSet<String>());
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }
        return buf.toString();
    }

    public void output(Appendable buf, Set<String> typesInProcess) throws AtlasException {
        try {
            buf.append("{name=").append(name);
            buf.append(", dataType=");
            dataType.output(buf, typesInProcess);
            buf.append(", multiplicity=").append(multiplicity.toString());
            buf.append(", isComposite=").append(Boolean.toString(isComposite));
            buf.append(", isUnique=").append(Boolean.toString(isUnique));
            buf.append(", isIndexable=").append(Boolean.toString(isIndexable));
            buf.append(", reverseAttributeName=").append(reverseAttributeName);
            buf.append('}');
        }
        catch(IOException e) {
            throw new AtlasException(e);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, multiplicity, isComposite, isUnique, isIndexable, reverseAttributeName, dataType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AttributeInfo that = (AttributeInfo) o;
        return isComposite == that.isComposite &&
                isUnique == that.isUnique &&
                isIndexable == that.isIndexable &&
                Objects.equals(name, that.name) &&
                Objects.equals(multiplicity, that.multiplicity) &&
                Objects.equals(reverseAttributeName, that.reverseAttributeName) &&
                dataType == null ? that.dataType == null : Objects.equals(dataType.getName(), that.dataType.getName());
    }

    public String toJson() throws JSONException {
        JSONObject json = new JSONObject();
        json.put("name", name);
        json.put("multiplicity", multiplicity.toJson());
        json.put("isComposite", isComposite);
        json.put("isUnique", isUnique);
        json.put("isIndexable", isIndexable);
        json.put("dataType", dataType.getName());
        json.put("reverseAttributeName", reverseAttributeName);
        return json.toString();
    }

    public static AttributeDefinition fromJson(String jsonStr) throws JSONException {
        JSONObject json = new JSONObject(jsonStr);
        String reverseAttr = null;
        boolean isSoftRef = false;
        if (json.has("reverseAttributeName")) {
            reverseAttr = json.getString("reverseAttributeName");
        }

        AttributeDefinition attributeDefinition = new AttributeDefinition(json.getString("name"), json.getString("dataType"),
                Multiplicity.fromJson(json.getString("multiplicity")), json.getBoolean("isComposite"),
                json.getBoolean("isUnique"), json.getBoolean("isIndexable"), reverseAttr);

        if (json.has("options")) {
            isSoftRef = getSoftRef(json);
            attributeDefinition.setSoftRef(isSoftRef);
        }

        return attributeDefinition;
    }

    private static boolean getSoftRef(JSONObject json) throws JSONException {
        final String SOFT_REF_KEY = "isSoftReference";

        boolean isSoftRef;
        Map map = AtlasType.fromJson(json.getString("options"), Map.class);
        isSoftRef = (map != null && map.containsKey(SOFT_REF_KEY)) ? Boolean.parseBoolean((String) map.get(SOFT_REF_KEY)) : false;
        return isSoftRef;
    }
}
