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
package org.apache.atlas.repository.impexp;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ImportTransforms {
    private static final Logger LOG = LoggerFactory.getLogger(ImportTransforms.class);

    private Map<String, Map<String, List<ImportTransformer>>> transforms;


    public static ImportTransforms fromJson(String jsonString) {
        ImportTransforms ret = null;

        if (StringUtils.isNotBlank(jsonString)) {
            ret = new ImportTransforms(jsonString);
        }

        return ret;
    }

    public Map<String, Map<String, List<ImportTransformer>>> getTransforms() {
        return transforms;
    }

    public Map<String, List<ImportTransformer>> getTransforms(String typeName) { return transforms.get(typeName); }

    public AtlasEntity.AtlasEntityWithExtInfo apply(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) throws AtlasBaseException {
        if (entityWithExtInfo != null) {
            apply(entityWithExtInfo.getEntity());

            if(MapUtils.isNotEmpty(entityWithExtInfo.getReferredEntities())) {
                for (AtlasEntity e : entityWithExtInfo.getReferredEntities().values()) {
                    apply(e);
                }
            }
        }

        return entityWithExtInfo;
    }

    public  AtlasEntity apply(AtlasEntity entity) throws AtlasBaseException {
        if(entity != null) {
            Map<String, List<ImportTransformer>> entityTransforms = getTransforms(entity.getTypeName());

            if (MapUtils.isNotEmpty(entityTransforms)) {
                for (Map.Entry<String, List<ImportTransformer>> entry : entityTransforms.entrySet()) {
                    String                   attributeName  = entry.getKey();
                    List<ImportTransformer> attrTransforms = entry.getValue();

                    if (!entity.hasAttribute(attributeName)) {
                        continue;
                    }

                    Object transformedValue = entity.getAttribute(attributeName);

                    for (ImportTransformer attrTransform : attrTransforms) {
                        transformedValue = attrTransform.apply(transformedValue);
                    }

                    entity.setAttribute(attributeName, transformedValue);
                }
            }
        }

        return entity;
    }

    private ImportTransforms() {
        transforms = new HashMap<>();
    }

    private ImportTransforms(String jsonString) {
        this();

        if(jsonString != null) {
            Map typeTransforms = AtlasType.fromJson(jsonString, Map.class);

            if (MapUtils.isNotEmpty(typeTransforms)) {
                for (Object key : typeTransforms.keySet()) {
                    Object              value               = typeTransforms.get(key);
                    String              entityType          = (String) key;
                    Map<String, Object> attributeTransforms = (Map<String, Object>)value;

                    if (MapUtils.isNotEmpty(attributeTransforms)) {
                        for (Map.Entry<String, Object> e : attributeTransforms.entrySet()) {
                            String       attributeName = e.getKey();
                            List<String> transforms    = (List<String>)e.getValue();

                            if (CollectionUtils.isNotEmpty(transforms)) {
                                for (String transform : transforms) {
                                    ImportTransformer transformers = null;

                                    try {
                                        transformers = ImportTransformer.getTransformer(transform);
                                    } catch (AtlasBaseException ex) {
                                        LOG.error("Error converting string to ImportTransformer: {}", transform, ex);
                                    }

                                    if (transformers != null) {
                                        add(entityType, attributeName, transformers);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private void add(String typeName, String attributeName, ImportTransformer transformer) {
        Map<String, List<ImportTransformer>> attrMap;

        if(transforms.containsKey(typeName)) {
            attrMap = transforms.get(typeName);
        } else {
            attrMap = new HashMap<>();
            transforms.put(typeName, attrMap);
        }

        List<ImportTransformer> list;
        if(attrMap.containsKey(attributeName)) {
            list = attrMap.get(attributeName);
        } else {
            list = new ArrayList<>();
            attrMap.put(attributeName, list);
        }

        list.add(transformer);
    }
}
