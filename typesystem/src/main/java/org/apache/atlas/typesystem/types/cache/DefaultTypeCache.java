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
package org.apache.atlas.typesystem.types.cache;

import com.google.inject.Singleton;
import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.apache.atlas.typesystem.types.EnumType;
import org.apache.atlas.typesystem.types.HierarchicalType;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.StructType;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Caches the types in-memory within the same process space.
 */
@Singleton
@SuppressWarnings("rawtypes")
public class DefaultTypeCache implements TypeCache {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultTypeCache.class);

    private Map<String, IDataType> types_ = new ConcurrentHashMap<>();
    private static final List<TypeCategory> validTypeFilterCategories =
            Arrays.asList(TypeCategory.CLASS, TypeCategory.TRAIT, TypeCategory.ENUM, TypeCategory.STRUCT);
    private static final List<TypeCategory> validSupertypeFilterCategories =
            Arrays.asList(TypeCategory.CLASS, TypeCategory.TRAIT);

    /*
     * (non-Javadoc)
     * @see
     * org.apache.atlas.typesystem.types.cache.TypeCache#has(java.lang
     * .String)
     */
    @Override
    public boolean has(String typeName) throws AtlasException {

        return types_.containsKey(typeName);
    }

    /* (non-Javadoc)
     * @see org.apache.atlas.typesystem.types.cache.TypeCache#has(org.
     * apache.atlas.typesystem.types.DataTypes.TypeCategory, java.lang.String)
     */
    @Override
    public boolean has(TypeCategory typeCategory, String typeName)
            throws AtlasException {

        assertValidTypeCategory(typeCategory);
        return has(typeName);
    }

    private void assertValidTypeCategory(String typeCategory) {
        assertValidTypeCategory(TypeCategory.valueOf(typeCategory));
    }

    private void assertValidTypeCategory(TypeCategory typeCategory) {
        // there might no need of 'typeCategory' in this implementation for
        // certain API, but for a distributed cache, it might help for the
        // implementers to partition the types per their category
        // while persisting so that look can be efficient

        if (typeCategory == null) {
            throw new IllegalArgumentException("Category of the types to be filtered is null.");
        }

        if (!validTypeFilterCategories.contains(typeCategory)) {
            throw new IllegalArgumentException("Category of the types should be one of " +
                    StringUtils.join(validTypeFilterCategories, ", "));
        }
    }

    /*
     * (non-Javadoc)
     * @see
     * org.apache.atlas.typesystem.types.cache.TypeCache#get(java.lang
     * .String)
     */
    @Override
    public IDataType get(String typeName) throws AtlasException {

        return types_.get(typeName);
    }

    /* (non-Javadoc)
     * @see org.apache.atlas.typesystem.types.cache.TypeCache#get(org.apache.
     * atlas.typesystem.types.DataTypes.TypeCategory, java.lang.String)
     */
    @Override
    public IDataType get(TypeCategory typeCategory, String typeName) throws AtlasException {

        assertValidTypeCategory(typeCategory);
        return get(typeName);
    }

    /**
     * Return the list of type names in the type system which match the specified filter.
     *
     * @return list of type names
     * @param filterMap - Map of filter for type names. Valid keys are CATEGORY, SUPERTYPE, NOT_SUPERTYPE
     * For example, CATEGORY = TRAIT && SUPERTYPE contains 'X' && SUPERTYPE !contains 'Y'
     */
    @Override
    public Collection<String> getTypeNames(Map<TYPE_FILTER, String> filterMap) throws AtlasException {
        assertFilter(filterMap);

        List<String> typeNames = new ArrayList<>();
        for (IDataType type : types_.values()) {
            if (shouldIncludeType(type, filterMap)) {
                typeNames.add(type.getName());
            }
        }
        return typeNames;
    }

    private boolean shouldIncludeType(IDataType type, Map<TYPE_FILTER, String> filterMap) {
        if (filterMap == null) {
            return true;
        }

        for (Entry<TYPE_FILTER, String> filterEntry : filterMap.entrySet()) {
            switch (filterEntry.getKey()) {
            case CATEGORY:
                if (!filterEntry.getValue().equals(type.getTypeCategory().name())) {
                    return false;
                }
                break;

            case SUPERTYPE:
                if (!validSupertypeFilterCategories.contains(type.getTypeCategory()) ||
                        !((HierarchicalType) type).getAllSuperTypeNames().contains(filterEntry.getValue())) {
                    return false;
                }
                break;

            case NOT_SUPERTYPE:
                if (!validSupertypeFilterCategories.contains(type.getTypeCategory()) ||
                        type.getName().equals(filterEntry.getValue()) ||
                        ((HierarchicalType) type).getAllSuperTypeNames().contains(filterEntry.getValue())) {
                    return false;
                }
                break;
            }
        }
        return true;
    }


    private void assertFilter(Map<TYPE_FILTER, String> filterMap) throws AtlasException {
        if (filterMap == null) {
            return;
        }

        for (Entry<TYPE_FILTER, String> filterEntry : filterMap.entrySet()) {
            switch (filterEntry.getKey()) {
            case CATEGORY:
                assertValidTypeCategory(filterEntry.getValue());
                break;

            case SUPERTYPE:
            case NOT_SUPERTYPE:
                if (!has(filterEntry.getValue())) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("{}: supertype does not exist", filterEntry.getValue());
                    }
                }
                break;

            default:
                throw new IllegalStateException("Unhandled filter " + filterEntry.getKey());
            }
        }
    }

    /*
     * (non-Javadoc)
     * @see
     * org.apache.atlas.typesystem.types.cache.TypeCache#getAllNames()
     */
    @Override
    public Collection<String> getAllTypeNames() throws AtlasException {

        return types_.keySet();
    }

    /*
     * (non-Javadoc)
     * @see
     * org.apache.atlas.typesystem.types.cache.TypeCache#put(org.apache
     * .atlas.typesystem.types.IDataType)
     */
    @Override
    public void put(IDataType type) throws AtlasException {

        assertValidType(type);
        types_.put(type.getName(), type);
    }

    private void assertValidType(IDataType type) throws
        AtlasException {

        if (type == null) {
            throw new AtlasException("type is null.");
        }

        boolean validTypeCategory = (type instanceof ClassType) ||
            (type instanceof TraitType) ||
            (type instanceof EnumType) ||
            (type instanceof StructType);

        if (!validTypeCategory) {
            throw new AtlasException("Category of the types should be one of ClassType | "
                + "TraitType | EnumType | StructType.");
        }
    }

    /*
     * (non-Javadoc)
     * @see
     * org.apache.atlas.typesystem.types.cache.TypeCache#putAll(java
     * .util.Collection)
     */
    @Override
    public void putAll(Collection<IDataType> types) throws AtlasException {

        for (IDataType type : types) {
            assertValidType(type);
            types_.put(type.getName(), type);
        }
    }

    /*
     * (non-Javadoc)
     * @see
     * org.apache.atlas.typesystem.types.cache.TypeCache#remove(java
     * .lang.String)
     */
    @Override
    public void remove(String typeName) throws AtlasException {

        types_.remove(typeName);
    }

    /* (non-Javadoc)
     * @see org.apache.atlas.typesystem.types.cache.TypeCache#remove(org.
     * apache.atlas.typesystem.types.DataTypes.TypeCategory, java.lang.String)
     */
    @Override
    public void remove(TypeCategory typeCategory, String typeName)
            throws AtlasException {

        assertValidTypeCategory(typeCategory);
        remove(typeName);
    }

    /*
     * (non-Javadoc)
     * @see org.apache.atlas.typesystem.types.cache.TypeCache#clear()
     */
    @Override
    public void clear() {

        types_.clear();
    }
}
