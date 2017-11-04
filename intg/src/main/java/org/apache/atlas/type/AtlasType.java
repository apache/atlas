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
import org.apache.atlas.model.v1.typedef.Multiplicity;
import org.codehaus.jackson.*;
import org.codehaus.jackson.map.*;
import org.codehaus.jackson.map.module.SimpleModule;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.List;




/**
 * base class that declares interface for all Atlas types.
 */

public abstract class AtlasType {

    private static final ObjectMapper mapper = new ObjectMapper()
                                            .configure(DeserializationConfig.Feature.USE_BIG_DECIMAL_FOR_FLOATS, true);

    private static final ObjectMapper mapperV1 = new ObjectMapper()
                                            .configure(DeserializationConfig.Feature.USE_BIG_DECIMAL_FOR_FLOATS, true);

    static {
        SimpleModule atlasSerDeModule = new SimpleModule("AtlasSerDe", new Version(1, 0, 0, null));

        atlasSerDeModule.addSerializer(Date.class, new DateSerializer());
        atlasSerDeModule.addDeserializer(Date.class, new DateDeserializer());
        atlasSerDeModule.addSerializer(Multiplicity.class, new MultiplicitySerializer());
        atlasSerDeModule.addDeserializer(Multiplicity.class, new MultiplicityDeserializer());

        mapperV1.registerModule(atlasSerDeModule);
    }


    private final String       typeName;
    private final TypeCategory typeCategory;

    protected AtlasType(AtlasBaseTypeDef typeDef) {
        this(typeDef.getName(), typeDef.getCategory());
    }

    protected AtlasType(String typeName, TypeCategory typeCategory) {
        this.typeName     = typeName;
        this.typeCategory = typeCategory;
    }

    void resolveReferences(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
    }

    void resolveReferencesPhase2(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
    }

    void resolveReferencesPhase3(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
    }

    public String getTypeName() { return typeName; }

    public TypeCategory getTypeCategory() { return typeCategory; }

    public abstract Object createDefaultValue();

    public Object createOptionalDefaultValue() {
        return createDefaultValue();
    }

    public Object createDefaultValue(Object val){
        return val == null ? createDefaultValue() : getNormalizedValue(val);
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

    public static String toV1Json(Object obj) {
        String ret;
        try {
            ret = mapperV1.writeValueAsString(obj);
        }catch (IOException e){
            ret = null;
        }
        return ret;
    }

    public static <T> T fromV1Json(String jsonStr, Class<T> type) {
        T ret;
        try {
            ret =  mapperV1.readValue(jsonStr, type);
        }catch (IOException e){
            ret = null;
        }
        return ret;
    }

    static class DateSerializer extends JsonSerializer<Date> {
        @Override
        public void serialize(Date value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            if (value != null) {
                jgen.writeString(AtlasBaseTypeDef.DATE_FORMATTER.format(value));
            }
        }
    }

    static class DateDeserializer extends JsonDeserializer<Date> {
        @Override
        public Date deserialize(JsonParser parser, DeserializationContext context) throws IOException {
            Date ret = null;

            String value = parser.readValueAs(String.class);

            if (value != null) {
                try {
                    ret = AtlasBaseTypeDef.DATE_FORMATTER.parse(value);
                } catch (ParseException excp) {
                }
            }

            return ret;
        }
    }

    static class MultiplicitySerializer extends JsonSerializer<Multiplicity> {
        @Override
        public void serialize(Multiplicity value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            if (value != null) {
                if (value.equals(Multiplicity.REQUIRED)) {
                    jgen.writeString("required");
                } else if (value.equals(Multiplicity.OPTIONAL)) {
                    jgen.writeString("optional");
                } else if (value.equals(Multiplicity.COLLECTION)) {
                    jgen.writeString("collection");
                } else if (value.equals(Multiplicity.SET)) {
                    jgen.writeString("set");
                }
            }
        }
    }

    static class MultiplicityDeserializer extends JsonDeserializer<Multiplicity> {
        @Override
        public Multiplicity deserialize(JsonParser parser, DeserializationContext context) throws IOException {
            Multiplicity ret = null;

            String value = parser.readValueAs(String.class);

            if (value != null) {
                if (value.equals("required")) {
                    ret = new Multiplicity(Multiplicity.REQUIRED);
                } else if (value.equals("optional")) {
                    ret = new Multiplicity(Multiplicity.OPTIONAL);
                } else if (value.equals("collection")) {
                    ret = new Multiplicity(Multiplicity.COLLECTION);
                } else if (value.equals("set")) {
                    ret = new Multiplicity(Multiplicity.SET);
                }
            }

            if (ret == null) {
                ret = new Multiplicity();
            }

            return ret;
        }
    }
}
