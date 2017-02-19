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
package org.apache.atlas.type;


import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.codehaus.jackson.map.ObjectMapper;


import java.io.IOException;
import java.util.List;




/**
 * base class that declares interface for all Atlas types.
 */

public abstract class AtlasType {

    private static final ObjectMapper mapper = new ObjectMapper();

    private final String       typeName;
    private final TypeCategory typeCategory;

    protected AtlasType(AtlasBaseTypeDef typeDef) {
        this(typeDef.getName(), typeDef.getCategory());
    }

    protected AtlasType(String typeName, TypeCategory typeCategory) {
        this.typeName     = typeName;
        this.typeCategory = typeCategory;
    }

    public void resolveReferences(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
    }

    public void resolveReferencesPhase2(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
    }

    public String getTypeName() { return typeName; }

    public TypeCategory getTypeCategory() { return typeCategory; }

    public abstract Object createDefaultValue();

    public Object createOptionalDefaultValue() {
        return createDefaultValue();
    }

    public abstract boolean isValidValue(Object obj);

    public abstract Object getNormalizedValue(Object obj);

    public boolean validateValue(Object obj, String objName, List<String> messages) {
        boolean ret = isValidValue(obj);

        if (!ret) {
            messages.add(objName + "=" + obj + ": invalid value for type " + getTypeName());
        }

        return ret;
    }

    public boolean isValidValueForUpdate(Object obj) { return isValidValue(obj); }

    public Object getNormalizedValueForUpdate(Object obj) { return getNormalizedValue(obj); }

    public boolean validateValueForUpdate(Object obj, String objName, List<String> messages) {
        return validateValue(obj, objName, messages);
    }

    /* for attribute of entity-type, the value would be of AtlasObjectId
     * when an attribute instance is created i.e. AtlasAttribute, this method
     * will be called to get AtlasEntityType replaced with AtlasObjectType
     */
    public AtlasType getTypeForAttribute() {
        return this;
    }

    public static String toJson(Object obj) {
        String ret;
        try {
            ret = mapper.writeValueAsString(obj);
        }catch (IOException e){
            ret = null;
        }
        return ret;
    }

    public static <T> T fromJson(String jsonStr, Class<T> type) {
        T ret;
        try {
            ret =  mapper.readValue(jsonStr, type);
        }catch (IOException e){
            ret = null;
        }
        return ret;
    }
}
