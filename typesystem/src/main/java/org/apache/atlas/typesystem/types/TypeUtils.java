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

import com.google.common.collect.ImmutableList;
import org.apache.atlas.AtlasException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TypeUtils {

    public static final String NAME_REGEX = "[a-zA-z][a-zA-Z0-9_]*";
    public static final Pattern NAME_PATTERN = Pattern.compile(NAME_REGEX);
    public static final Pattern ARRAY_TYPE_NAME_PATTERN = Pattern.compile(String.format("array<(%s)>", NAME_REGEX));
    public static final Pattern MAP_TYPE_NAME_PATTERN =
            Pattern.compile(String.format("map<(%s),(%s)>", NAME_REGEX, NAME_REGEX));

    public static void outputVal(String val, Appendable buf, String prefix) throws AtlasException {
        try {
            buf.append(prefix).append(val);
        } catch (IOException ie) {
            throw new AtlasException(ie);
        }
    }

    public static String parseAsArrayType(String typeName) {
        Matcher m = ARRAY_TYPE_NAME_PATTERN.matcher(typeName);
        return m.matches() ? m.group(1) : null;
    }

    public static String[] parseAsMapType(String typeName) {
        Matcher m = MAP_TYPE_NAME_PATTERN.matcher(typeName);
        return m.matches() ? new String[]{m.group(1), m.group(2)} : null;
    }

    public static Map<AttributeInfo, List<String>> buildAttrInfoToNameMap(FieldMapping f) {
        Map<AttributeInfo, List<String>> b = new HashMap();
        for (Map.Entry<String, AttributeInfo> e : f.fields.entrySet()) {
            List<String> names = b.get(e.getValue());
            if (names == null) {
                names = new ArrayList<>();
                b.put(e.getValue(), names);
            }
            names.add(e.getKey());
        }
        return b;
    }

    protected static class Pair<L, R> {
        protected L left;
        protected R right;

        public Pair(L left, R right) {
            this.left = left;
            this.right = right;
        }
    }

    /**
     * Validates that the old field mapping can be replaced with new field mapping
     * @param oldFieldMapping
     * @param newFieldMapping
     */
    public static void validateUpdate(FieldMapping oldFieldMapping, FieldMapping newFieldMapping)
            throws TypeUpdateException {
        Map<String, AttributeInfo> newFields = newFieldMapping.fields;
        for (AttributeInfo attribute : oldFieldMapping.fields.values()) {
            if (newFields.containsKey(attribute.name)) {
                AttributeInfo newAttribute = newFields.get(attribute.name);
                //If old attribute is also in new definition, only allowed change is multiplicity change from REQUIRED to OPTIONAL
                if (!newAttribute.equals(attribute)) {
                    if (attribute.multiplicity == Multiplicity.REQUIRED
                            && newAttribute.multiplicity == Multiplicity.OPTIONAL) {
                        continue;
                    } else {
                        throw new TypeUpdateException("Attribute " + attribute.name + " can't be updated");
                    }
                }

            } else {
                //If old attribute is missing in new definition, return false as attributes can't be deleted
                throw new TypeUpdateException("Old Attribute " + attribute.name + " is missing");
            }
        }

        //Only new attributes
        Set<String> newAttributes = new HashSet<>(ImmutableList.copyOf(newFields.keySet()));
        newAttributes.removeAll(oldFieldMapping.fields.keySet());
        for (String attributeName : newAttributes) {
            AttributeInfo newAttribute = newFields.get(attributeName);
            //New required attribute can't be added
            if (newAttribute.multiplicity == Multiplicity.REQUIRED) {
                throw new TypeUpdateException("Can't add required attribute " + attributeName);
            }
        }
    }
}
