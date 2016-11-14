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
package org.apache.atlas.web.adapters;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Singleton
public class AtlasFormatConverters {

    public static String VERSION_V1 = "v1";
    public static String VERSION_V2 = "v2";

    private Map<String, AtlasFormatAdapter> registry = new HashMap<>();

    public void registerConverter(AtlasFormatAdapter adapter, String sourceVersion, String targetVersion) {
        registry.put(getKey(sourceVersion, targetVersion, adapter.getTypeCategory()), adapter);
    }

    public AtlasFormatAdapter getConverter(String sourceVersion, String targetVersion, TypeCategory typeCategory) throws AtlasBaseException {
        if (registry.containsKey(getKey(sourceVersion, targetVersion, typeCategory))) {
            return registry.get(getKey(sourceVersion, targetVersion, typeCategory));
        }
        throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, "Could not find the converter for this type " + typeCategory);
    }

    public static boolean isArrayListType(Class c) {
        return List.class.isAssignableFrom(c);
    }

    public static boolean isSetType(Class c) {
        return Set.class.isAssignableFrom(c);
    }

    public static boolean isPrimitiveType(final Class c) {
        if (c != null) {
            if (Number.class.isAssignableFrom(c)) {
                return true;
            }

            if (String.class.isAssignableFrom(c)) {
                return true;
            }

            if (Date.class.isAssignableFrom(c)) {
                return true;
            }

            return c.isPrimitive();
        }
        return false;
    }

    public static boolean isMapType(Object o) {
        if ( o != null ) {
            return Map.class.isAssignableFrom(o.getClass());
        }
        return false;
    }

    String getKey(String sourceVersion, String targetVersion, TypeCategory typeCategory) {
        return sourceVersion + "-to-" + targetVersion + "-" + typeCategory.name();
    }
}
