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

import org.apache.atlas.AtlasException;
import org.apache.atlas.catalog.exception.CatalogRuntimeException;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.typesystem.types.FieldMapping;
import org.apache.atlas.typesystem.types.HierarchicalType;
import org.apache.atlas.typesystem.types.TypeSystem;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Default property mapper which translates property names to/from name exposed in API to internal fully qualified name.
 */
public class DefaultPropertyMapper implements PropertyMapper {
    //todo: abstract HierarchicalType
    private Map<String, HierarchicalType> typeInstances = new HashMap<>();
    private final Map<String, String> m_qualifiedToCleanMap = new HashMap<>();
    private final Map<String, String> m_cleanToQualifiedMap = new HashMap<>();


    public DefaultPropertyMapper() {
        this(Collections.<String, String>emptyMap(), Collections.<String, String>emptyMap());
    }

    public DefaultPropertyMapper(Map<String, String> qualifiedToCleanMap,
                                 Map<String, String> cleanToQualifiedMap) {
        setDefaultMappings();

        m_qualifiedToCleanMap.putAll(qualifiedToCleanMap);
        m_cleanToQualifiedMap.putAll(cleanToQualifiedMap);
    }

    @Override
    public String toCleanName(String propName, String type) {
        HierarchicalType dataType = getDataType(type);
        String replacement = m_qualifiedToCleanMap.get(propName);
        if (replacement == null && dataType != null) {
            FieldMapping fieldMap = dataType.fieldMapping();
            if (! fieldMap.fields.containsKey(propName) && propName.contains(".")) {
                String cleanName = propName.substring(propName.lastIndexOf('.') + 1);
                if (fieldMap.fields.containsKey(cleanName)) {
                    replacement = cleanName;
                }
            }
        }

        if (replacement == null) {
            replacement = propName;
        }
        return replacement;
    }

    @Override
    public String toFullyQualifiedName(String propName, String type) {
        HierarchicalType dataType = getDataType(type);
        String replacement = m_cleanToQualifiedMap.get(propName);
        if (replacement == null && dataType != null) {
            FieldMapping fieldMap = dataType.fieldMapping();
            if (fieldMap.fields.containsKey(propName)) {
                try {
                    replacement = dataType.getQualifiedName(propName);
                } catch (AtlasException e) {
                    throw new CatalogRuntimeException(String.format(
                            "Unable to resolve fully qualified property name for type '%s': %s", type, e), e);
                }
            }
        }

        if (replacement == null) {
            replacement = propName;
        }
        return replacement;
    }

    //todo: abstract this via AtlasTypeSystem
    protected synchronized HierarchicalType getDataType(String type) {
        HierarchicalType dataType = typeInstances.get(type);
        //todo: are there still cases where type can be null?
        if (dataType == null) {
            dataType = createDataType(type);
            typeInstances.put(type, dataType);
        }
        return dataType;
    }

    protected HierarchicalType createDataType(String type) {
        try {
            return TypeSystem.getInstance().getDataType(HierarchicalType.class, type);
        } catch (AtlasException e) {
            throw new CatalogRuntimeException("Unable to get type instance from type system for type: " + type, e);
        }
    }

    private void setDefaultMappings() {
        //todo: these are all internal "__*" properties
        //todo: should be able to ask type system for the "clean" name for these
        m_qualifiedToCleanMap.put(Constants.GUID_PROPERTY_KEY, "id");
        m_cleanToQualifiedMap.put("id", Constants.GUID_PROPERTY_KEY);

        m_qualifiedToCleanMap.put(Constants.TIMESTAMP_PROPERTY_KEY, "creation_time");
        m_cleanToQualifiedMap.put("creation_time", Constants.TIMESTAMP_PROPERTY_KEY);

        m_qualifiedToCleanMap.put(Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, "modified_time");
        m_cleanToQualifiedMap.put("modified_time", Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY);

        m_qualifiedToCleanMap.put(Constants.ENTITY_TYPE_PROPERTY_KEY, "type");
        m_cleanToQualifiedMap.put("type", Constants.ENTITY_TYPE_PROPERTY_KEY);

        m_qualifiedToCleanMap.put(Constants.VERSION_PROPERTY_KEY, "version");
        m_cleanToQualifiedMap.put("version", Constants.VERSION_PROPERTY_KEY);

        m_qualifiedToCleanMap.put(Constants.TRAIT_NAMES_PROPERTY_KEY, "trait_names");
        m_cleanToQualifiedMap.put("trait_names", Constants.TRAIT_NAMES_PROPERTY_KEY);

        m_qualifiedToCleanMap.put(Constants.SUPER_TYPES_PROPERTY_KEY, "super_types");
        m_cleanToQualifiedMap.put("super_types", Constants.SUPER_TYPES_PROPERTY_KEY);

        m_qualifiedToCleanMap.put(Constants.STATE_PROPERTY_KEY, "state");
        m_cleanToQualifiedMap.put("state", Constants.STATE_PROPERTY_KEY);
    }
}
