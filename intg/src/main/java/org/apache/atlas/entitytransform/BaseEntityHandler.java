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
package org.apache.atlas.entitytransform;

import org.apache.atlas.model.impexp.AttributeTransform;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.atlas.entitytransform.TransformationConstants.TYPE_NAME_ATTRIBUTE_NAME_SEP;

public class BaseEntityHandler {
    private static final Logger LOG = LoggerFactory.getLogger(BaseEntityHandler.class);

    protected final List<AtlasEntityTransformer> transformers;
    protected final boolean                      hasCustomAttributeTransformer;

    public BaseEntityHandler(List<AtlasEntityTransformer> transformers) {
        this(transformers, null);
    }

    public BaseEntityHandler(List<AtlasEntityTransformer> transformers, List<String> customTransformAttributes) {
        this.transformers                  = transformers;
        this.hasCustomAttributeTransformer = hasTransformerForAnyAttribute(customTransformAttributes);
    }

    public boolean hasCustomAttributeTransformer() {
        return hasCustomAttributeTransformer;
    }

    public AtlasEntity transform(AtlasEntity entity) {
        if (CollectionUtils.isNotEmpty(transformers)) {
            AtlasTransformableEntity transformableEntity = getTransformableEntity(entity);

            if (transformableEntity != null) {
                for (AtlasEntityTransformer transformer : transformers) {
                    transformer.transform(transformableEntity);
                }

                transformableEntity.transformComplete();
            }
        }

        return entity;
    }

    public AtlasTransformableEntity getTransformableEntity(AtlasEntity entity) {
        return new AtlasTransformableEntity(entity);
    }

    public static List<BaseEntityHandler> createEntityHandlers(List<AttributeTransform> transforms) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> BaseEntityHandler.createEntityHandlers(transforms={})", transforms);
        }

        List<AtlasEntityTransformer> transformers = new ArrayList<>();

        for (AttributeTransform transform : transforms) {
            transformers.add(new AtlasEntityTransformer(transform));
        }

        BaseEntityHandler[] handlers = new BaseEntityHandler[] {
                new HdfsPathEntityHandler(transformers),
                new HiveDatabaseEntityHandler(transformers),
                new HiveTableEntityHandler(transformers),
                new HiveColumnEntityHandler(transformers),
                new HiveStorageDescriptorEntityHandler(transformers)
        };

        List<BaseEntityHandler> ret = new ArrayList<>();

        // include customer handlers, only if its customer attribute is transformed
        for (BaseEntityHandler handler : handlers) {
            if (handler.hasCustomAttributeTransformer()) {
                ret.add(handler);
            }
        }

        if (CollectionUtils.isEmpty(ret)) {
            ret.add(new BaseEntityHandler(transformers));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== BaseEntityHandler.createEntityHandlers(transforms={}): ret.size={}", transforms, ret.size());
        }

        return ret;
    }

    private boolean hasTransformerForAnyAttribute(List<String> attributes) {
        if (CollectionUtils.isNotEmpty(transformers) && CollectionUtils.isNotEmpty(attributes)) {
            for (AtlasEntityTransformer transformer : transformers) {
                for (Action action : transformer.getActions()) {
                    if (attributes.contains(action.getAttributeName())) {
                        return true;
                    }
                }
            }
        }

        return false;
    }


    public static class AtlasTransformableEntity {
        protected final AtlasEntity entity;

        protected AtlasTransformableEntity(AtlasEntity entity) {
            this.entity = entity;
        }

        public AtlasEntity getEntity() {
            return entity;
        }

        public Object getAttribute(String attributeName) {
            Object ret = null;

            if (entity != null && attributeName != null) {
                ret = entity.getAttribute(attributeName);

                if (ret == null) { // try after dropping typeName prefix, if attributeName contains it
                    int idxSep = attributeName.indexOf(TYPE_NAME_ATTRIBUTE_NAME_SEP);

                    if (idxSep != -1) {
                        ret = entity.getAttribute(attributeName.substring(idxSep + 1));
                    }
                }
            }

            return ret;
        }

        public void setAttribute(String attributeName, String attributeValue) {
            if (entity != null && attributeName != null) {
                int idxSep = attributeName.indexOf(TYPE_NAME_ATTRIBUTE_NAME_SEP); // drop typeName prefix, if attributeName contains it

                if (idxSep != -1) {
                    entity.setAttribute(attributeName.substring(idxSep + 1), attributeValue);
                } else {
                    entity.setAttribute(attributeName, attributeValue);
                }
            }
        }

        public boolean hasAttribute(String attributeName) {
            return getAttribute(attributeName) != null;
        }

        public void transformComplete() {
            // implementations can override to set value of computed-attributes
        }
    }
}