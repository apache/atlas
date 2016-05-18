/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.catalog;

import com.google.gson.stream.JsonWriter;
import org.apache.atlas.catalog.exception.CatalogRuntimeException;

import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * JSON serializer.
 */
public class JsonSerializer {
    public String serialize(Result result, UriInfo ui) {
        Writer json = new StringWriter();
        JsonWriter writer = new JsonWriter(json);
        writer.setIndent("    ");

        try {
            writeValue(writer, result.getPropertyMaps(), ui.getBaseUri().toASCIIString());
        } catch (IOException e) {
            throw new CatalogRuntimeException("Unable to write JSON response.", e);
        }
        return json.toString();
    }

    private void writeValue(JsonWriter writer, Object value, String baseUrl) throws IOException {
        if (value == null) {
            writer.nullValue();
        } else if (value instanceof Map) {
            writer.beginObject();
            LinkedHashMap<String, Object> nonScalarMap = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : ((Map<String, Object>) value).entrySet()) {
                String key = entry.getKey();
                Object val = entry.getValue();

                if (val == null || ! (val instanceof Collection || val instanceof Map)) {
                    //todo: use a token in value instead of prop name
                    if (key.equals("href")) {
                        val = baseUrl + String.valueOf(val);
                    }
                    writer.name(key);
                    writeValue(writer, val, baseUrl);
                } else {
                    nonScalarMap.put(key, val);
                }
            }
            for (Map.Entry<String, Object> entry : nonScalarMap.entrySet()) {
                writer.name(entry.getKey());
                writeValue(writer, entry.getValue(), baseUrl);
            }
            writer.endObject();
        } else if (value instanceof Collection) {
            writer.beginArray();
            for (Object o : (Collection) value) {
                writeValue(writer, o, baseUrl);
            }
            writer.endArray();
        } else if (value instanceof Number) {
            writer.value((Number) value);
        } else if (value instanceof Boolean) {
            writer.value((Boolean) value);
        } else {
            // everything else is String
            writer.value(String.valueOf(value));
        }
    }
}
