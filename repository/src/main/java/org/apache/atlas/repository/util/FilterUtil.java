/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.util;

import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.collections.PredicateUtils;
import org.apache.commons.collections.functors.NotPredicate;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class FilterUtil {
    public static Predicate getPredicateFromSearchFilter(SearchFilter searchFilter) {
        List<Predicate> predicates = new ArrayList<>();

        final String       type         = searchFilter.getParam(SearchFilter.PARAM_TYPE);
        final String       name         = searchFilter.getParam(SearchFilter.PARAM_NAME);
        final String       supertype    = searchFilter.getParam(SearchFilter.PARAM_SUPERTYPE);
        final String       notSupertype = searchFilter.getParam(SearchFilter.PARAM_NOT_SUPERTYPE);
        final List<String> notNames     = searchFilter.getParams(SearchFilter.PARAM_NOT_NAME);

        // Add filter for the type/category
        if (StringUtils.isNotBlank(type)) {
            predicates.add(getTypePredicate(type));
        }

        // Add filter for the name
        if (StringUtils.isNotBlank(name)) {
            predicates.add(getNamePredicate(name));
        }

        // Add filter for the supertype
        if (StringUtils.isNotBlank(supertype)) {
            predicates.add(getSuperTypePredicate(supertype));
        }

        // Add filter for the supertype negation
        if (StringUtils.isNotBlank(notSupertype)) {
            predicates.add(new NotPredicate(getSuperTypePredicate(notSupertype)));
        }

        // Add filter for the type negation
        if (CollectionUtils.isNotEmpty(notNames)) {
            for (String notName : notNames) {
                predicates.add(new NotPredicate(getNamePredicate(notName)));
            }
        }

        return PredicateUtils.allPredicate(predicates);
    }

    private static Predicate getNamePredicate(final String name) {
        return new Predicate() {
            private boolean isAtlasType(Object o) {
                return o instanceof AtlasType;
            }

            @Override
            public boolean evaluate(Object o) {
                return o != null && isAtlasType(o) && Objects.equals(((AtlasType) o).getTypeName(), name);
            }
        };
    }

    private static Predicate getSuperTypePredicate(final String supertype) {
        return new Predicate() {
            private boolean isClassificationType(Object o) {
                return o instanceof AtlasClassificationType;
            }

            private boolean isEntityType(Object o) {
                return o instanceof AtlasEntityType;
            }

            @Override
            public boolean evaluate(Object o) {
                return (isClassificationType(o) && ((AtlasClassificationType) o).getAllSuperTypes().contains(supertype)) ||
                       (isEntityType(o) && ((AtlasEntityType) o).getAllSuperTypes().contains(supertype));
            }
        };
    }

    private static Predicate getTypePredicate(final String type) {
        return new Predicate() {
            @Override
            public boolean evaluate(Object o) {
                if (o instanceof AtlasType) {
                    AtlasType atlasType = (AtlasType) o;

                    switch (type.toUpperCase()) {
                        case "CLASS":
                        case "ENTITY":
                            return atlasType.getTypeCategory() == TypeCategory.ENTITY;
                        case "TRAIT":
                        case "CLASSIFICATION":
                            return atlasType.getTypeCategory() == TypeCategory.CLASSIFICATION;
                        case "STRUCT":
                            return atlasType.getTypeCategory() == TypeCategory.STRUCT;
                        case "ENUM":
                            return atlasType.getTypeCategory() == TypeCategory.ENUM;
                        case "RELATIONSHIP":
                            return atlasType.getTypeCategory() == TypeCategory.RELATIONSHIP;
                        default:
                            // This shouldn't have happened
                            return false;
                    }
                }
                return false;
            }
        };
    }

    public static void addParamsToHideInternalType(SearchFilter searchFilter) {
        searchFilter.setParam(SearchFilter.PARAM_NOT_NAME, Constants.TYPE_NAME_INTERNAL);
        searchFilter.setParam(SearchFilter.PARAM_NOT_SUPERTYPE, Constants.TYPE_NAME_INTERNAL);
    }
}
