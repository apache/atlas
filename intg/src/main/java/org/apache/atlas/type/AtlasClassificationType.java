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


import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * class that implements behaviour of a classification-type.
 */
public class AtlasClassificationType extends AtlasStructType {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasClassificationType.class);

    private final AtlasClassificationDef classificationDef;

    private List<AtlasClassificationType>  superTypes        = Collections.emptyList();
    private Set<String>                    allSuperTypes     = Collections.emptySet();
    private Map<String, AtlasAttributeDef> allAttributeDefs  = Collections.emptyMap();
    private Map<String, AtlasType>         allAttributeTypes = new HashMap<>();

    public AtlasClassificationType(AtlasClassificationDef classificationDef) {
        super(classificationDef);

        this.classificationDef = classificationDef;
    }

    public AtlasClassificationType(AtlasClassificationDef classificationDef, AtlasTypeRegistry typeRegistry)
        throws AtlasBaseException {
        super(classificationDef);

        this.classificationDef = classificationDef;

        resolveReferences(typeRegistry);
    }

    public AtlasClassificationDef getClassificationDef() { return classificationDef; }

    @Override
    public void resolveReferences(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        super.resolveReferences(typeRegistry);

        List<AtlasClassificationType>  s    = new ArrayList<>();
        Set<String>                    allS = new HashSet<>();
        Map<String, AtlasAttributeDef> allA = new HashMap<>();

        getTypeHierarchyInfo(typeRegistry, allS, allA);

        for (String superTypeName : classificationDef.getSuperTypes()) {
            AtlasType superType = typeRegistry.getType(superTypeName);

            if (superType instanceof AtlasClassificationType) {
                s.add((AtlasClassificationType)superType);
            } else {
                throw new AtlasBaseException(AtlasErrorCode.INCOMPATIBLE_SUPERTYPE, superTypeName,
                        classificationDef.getName());
            }
        }

        this.superTypes        = Collections.unmodifiableList(s);
        this.allSuperTypes     = Collections.unmodifiableSet(allS);
        this.allAttributeDefs  = Collections.unmodifiableMap(allA);
        this.allAttributeTypes = new HashMap<>(); // this will be rebuilt on calls to getAttributeType()
    }

    public Set<String> getSuperTypes() {
        return classificationDef.getSuperTypes();
    }

    public Set<String> getAllSuperTypes() { return allSuperTypes; }

    public Map<String, AtlasAttributeDef> getAllAttributeDefs() { return allAttributeDefs; }

    @Override
    public AtlasType getAttributeType(String attributeName) {
        AtlasType ret = allAttributeTypes.get(attributeName);

        if (ret == null) {
            ret = super.getAttributeType(attributeName);

            if (ret == null) {
                for (AtlasClassificationType superType : superTypes) {
                    ret = superType.getAttributeType(attributeName);

                    if (ret != null) {
                        break;
                    }
                }
            }

            if (ret != null) {
                allAttributeTypes.put(attributeName, ret);
            }
        }

        return ret;
    }


    @Override
    public AtlasAttributeDef getAttributeDef(String attributeName) {
        AtlasAttributeDef ret = super.getAttributeDef(attributeName);

        if (ret == null) {
            for (AtlasClassificationType superType : superTypes) {
                ret = superType.getAttributeDef(attributeName);

                if (ret != null) {
                    break;
                }
            }
        }

        return ret;
    }

    public boolean isSuperTypeOf(AtlasClassificationType classificationType) {
        return classificationType != null && classificationType.getAllSuperTypes().contains(this.getTypeName());
    }

    public boolean isSubTypeOf(AtlasClassificationType classificationType) {
        return classificationType != null && allSuperTypes.contains(classificationType.getTypeName());
    }

    @Override
    public AtlasClassification createDefaultValue() {
        AtlasClassification ret = new AtlasClassification(classificationDef.getName());

        populateDefaultValues(ret);

        return ret;
    }

    @Override
    public boolean isValidValue(Object obj) {
        if (obj != null) {
            for (AtlasClassificationType superType : superTypes) {
                if (!superType.isValidValue(obj)) {
                    return false;
                }
            }

            return super.isValidValue(obj);
        }

        return true;
    }

    @Override
    public Object getNormalizedValue(Object obj) {
        Object ret = null;

        if (obj != null) {
            if (isValidValue(obj)) {
                if (obj instanceof AtlasClassification) {
                    normalizeAttributeValues((AtlasClassification) obj);
                    ret = obj;
                } else if (obj instanceof Map) {
                    normalizeAttributeValues((Map) obj);
                    ret = obj;
                }
            }
        }

        return ret;
    }

    @Override
    public boolean validateValue(Object obj, String objName, List<String> messages) {
        boolean ret = true;

        if (obj != null) {
            for (AtlasClassificationType superType : superTypes) {
                ret = superType.validateValue(obj, objName, messages) && ret;
            }

            ret = super.validateValue(obj, objName, messages) && ret;
        }

        return ret;
    }

    public void normalizeAttributeValues(AtlasClassification classification) {
        if (classification != null) {
            for (AtlasClassificationType superType : superTypes) {
                superType.normalizeAttributeValues(classification);
            }

            super.normalizeAttributeValues(classification);
        }
    }

    @Override
    public void normalizeAttributeValues(Map<String, Object> obj) {
        if (obj != null) {
            for (AtlasClassificationType superType : superTypes) {
                superType.normalizeAttributeValues(obj);
            }

            super.normalizeAttributeValues(obj);
        }
    }

    public void populateDefaultValues(AtlasClassification classification) {
        if (classification != null) {
            for (AtlasClassificationType superType : superTypes) {
                superType.populateDefaultValues(classification);
            }

            super.populateDefaultValues(classification);
        }
    }

    private void getTypeHierarchyInfo(AtlasTypeRegistry              typeRegistry,
                                      Set<String>                    allSuperTypeNames,
                                      Map<String, AtlasAttributeDef> allAttributeDefs) throws AtlasBaseException {
        List<String> visitedTypes = new ArrayList<>();

        collectTypeHierarchyInfo(typeRegistry, allSuperTypeNames, allAttributeDefs, visitedTypes);
    }

    /*
     * This method should not assume that resolveReferences() has been called on all superTypes.
     * this.classificationDef is the only safe member to reference here
     */
    private void collectTypeHierarchyInfo(AtlasTypeRegistry              typeRegistry,
                                          Set<String>                    allSuperTypeNames,
                                          Map<String, AtlasAttributeDef> allAttributeDefs,
                                          List<String>                   visitedTypes) throws AtlasBaseException {
        if (visitedTypes.contains(classificationDef.getName())) {
            throw new AtlasBaseException(AtlasErrorCode.CIRCULAR_REFERENCE, classificationDef.getName(),
                                         visitedTypes.toString());
        }

        if (CollectionUtils.isNotEmpty(classificationDef.getSuperTypes())) {
            visitedTypes.add(classificationDef.getName());
            for (String superTypeName : classificationDef.getSuperTypes()) {
                AtlasType type = typeRegistry.getType(superTypeName);

                if (type instanceof AtlasClassificationType) {
                    AtlasClassificationType superType = (AtlasClassificationType) type;

                    superType.collectTypeHierarchyInfo(typeRegistry, allSuperTypeNames, allAttributeDefs, visitedTypes);
                }
            }
            visitedTypes.remove(classificationDef.getName());

            allSuperTypeNames.addAll(classificationDef.getSuperTypes());
        }

        if (CollectionUtils.isNotEmpty(classificationDef.getAttributeDefs())) {
            for (AtlasAttributeDef attributeDef : classificationDef.getAttributeDefs()) {
                allAttributeDefs.put(attributeDef.getName(), attributeDef);
            }
        }
    }
}
