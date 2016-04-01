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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;


/**
 * Sorts hierarchical types by supertype dependency
 */
public class HierarchicalTypeDependencySorter {
    
    /**
     * Sorts the specified hierarchical types by supertype dependencies, 
     * such that any type A which is a supertype of type B
     * will always be located earlier in the result list; that is, the supertype 
     * A would be found at some index i and subtype B would be found at some index j,
     * and i < j.
     * 
     * @param types  hierarchical types to be sorted
     * @return hierarchical types sorted by supertype dependency
     */
    public static <T extends HierarchicalType> List<T> sortTypes(List<T> types) {
        Map<String, T> typesByName = new HashMap<>();
        for (T type : types) {
            typesByName.put(type.name, type);
        }
        List<T> result = new ArrayList<>(types.size());
        Set<T> processed = new HashSet<>();
        for (T type : types) {
            addToResult(type, result, processed, typesByName);
        }
        return result;
    }
    
    private static <T extends HierarchicalType> void addToResult(T type, List<T> result, 
        Set<T> processed, Map<String, T> typesByName) {
        
        if (processed.contains(type)) {
            return;
        }
        processed.add(type);
        ImmutableSet<String> superTypeNames = type.superTypes;
        for (String superTypeName : superTypeNames) {
            // Recursively add any supertypes first to the result.
            T superType = typesByName.get(superTypeName);
            if (superType != null) {
                addToResult(superType, result, processed, typesByName);
            }
        }
        result.add(type);
    }
}
