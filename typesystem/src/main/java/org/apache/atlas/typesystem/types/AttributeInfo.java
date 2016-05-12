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
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.Map;

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
    }

    public IDataType dataType() {
        return dataType;
    }

    void setDataType(IDataType dT) {
        dataType = dT;
    }

    @Override
    public String toString() {
        return "AttributeInfo{" +
                "name='" + name + '\'' +
                ", dataType=" + dataType +
                ", multiplicity=" + multiplicity +
                ", isComposite=" + isComposite +
                ", isUnique=" + isUnique +
                ", isIndexable=" + isIndexable +
                ", reverseAttributeName='" + reverseAttributeName + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AttributeInfo that = (AttributeInfo) o;

        if (isComposite != that.isComposite) {
            return false;
        }
        if (isUnique != that.isUnique) {
            return false;
        }
        if (isIndexable != that.isIndexable) {
            return false;
        }
        if (!dataType.getName().equals(that.dataType.getName())) {
            return false;
        }
        if (!multiplicity.equals(that.multiplicity)) {
            return false;
        }
        if (!name.equals(that.name)) {
            return false;
        }
        if (reverseAttributeName != null ? !reverseAttributeName.equals(that.reverseAttributeName) :
                that.reverseAttributeName != null) {
            return false;
        }

        return true;
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
        if (json.has("reverseAttributeName")) {
            reverseAttr = json.getString("reverseAttributeName");
        }
        return new AttributeDefinition(json.getString("name"), json.getString("dataType"),
                Multiplicity.fromJson(json.getString("multiplicity")), json.getBoolean("isComposite"),
                json.getBoolean("isUnique"), json.getBoolean("isIndexable"), reverseAttr);
    }
}
