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


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * class that implements behaviour of an entity-type.
 */
public class AtlasEntityType extends AtlasStructType {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasEntityType.class);

    private final AtlasEntityDef entityDef;

    private List<AtlasEntityType> superTypes    = Collections.emptyList();
    private Set<String>           allSuperTypes = Collections.emptySet();

    public AtlasEntityType(AtlasEntityDef entityDef) {
        super(entityDef, TypeCategory.ENTITY);

        this.entityDef = entityDef;
    }

    public AtlasEntityType(AtlasEntityDef entityDef, AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        super(entityDef);

        this.entityDef = entityDef;

        resolveReferences(typeRegistry);
    }

    @Override
    public void resolveReferences(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        super.resolveReferences(typeRegistry);

        List<AtlasEntityType> s    = new ArrayList<AtlasEntityType>();
        Set<String>           allS = new HashSet<String>();

        for (String superTypeName : entityDef.getSuperTypes()) {
            AtlasType superType = typeRegistry.getType(superTypeName);

            if (superType != null && superType instanceof AtlasEntityType) {
                AtlasEntityType superEntityType = (AtlasEntityType)superType;

                superEntityType.resolveReferences(typeRegistry);

                s.add(superEntityType);
                allS.add(superTypeName);

                if (CollectionUtils.isNotEmpty(superEntityType.getAllSuperTypes())) {
                    allS.addAll(superEntityType.getAllSuperTypes());
                }
            } else {
                String msg = superTypeName + ((superType == null) ? ": unknown" : ": incompatible");

                msg += (" supertype in entity " + entityDef.getName());

                LOG.error(msg);

                throw new AtlasBaseException(msg);
            }
        }

        this.superTypes    = Collections.unmodifiableList(s);
        this.allSuperTypes = Collections.unmodifiableSet(allS);
    }

    public Set<String> getSuperTypes() {
        return entityDef.getSuperTypes();
    }

    public Set<String> getAllSuperTypes() {
        return allSuperTypes;
    }

    public boolean isSuperTypeOf(AtlasEntityType entityType) {
        return entityType != null ? entityType.getAllSuperTypes().contains(this.getTypeName()) : false;
    }

    public boolean isSubTypeOf(AtlasEntityType entityType) {
        return entityType != null ? allSuperTypes.contains(entityType.getTypeName()) : false;
    }

    @Override
    public AtlasEntity createDefaultValue() {
        AtlasEntity ret = new AtlasEntity(entityDef.getName());

        populateDefaultValues(ret);

        return ret;
    }

    @Override
    public boolean isValidValue(Object obj) {
        if (obj != null) {
            for (AtlasEntityType superType : superTypes) {
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
                if (obj instanceof AtlasEntity) {
                    normalizeAttributeValues((AtlasEntity) obj);
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
            for (AtlasEntityType superType : superTypes) {
                ret = superType.validateValue(obj, objName, messages) && ret;
            }

            ret = super.validateValue(obj, objName, messages) && ret;
        }

        return ret;
    }

    public void normalizeAttributeValues(AtlasEntity ent) {
        if (ent != null) {
            for (AtlasEntityType superType : superTypes) {
                superType.normalizeAttributeValues(ent);
            }

            super.normalizeAttributeValues(ent);
        }
    }

    @Override
    public void normalizeAttributeValues(Map<String, Object> obj) {
        if (obj != null) {
            for (AtlasEntityType superType : superTypes) {
                superType.normalizeAttributeValues(obj);
            }

            super.normalizeAttributeValues(obj);
        }
    }

    public void populateDefaultValues(AtlasEntity ent) {
        if (ent != null) {
            for (AtlasEntityType superType : superTypes) {
                superType.populateDefaultValues(ent);
            }

            super.populateDefaultValues(ent);
        }
    }
}
