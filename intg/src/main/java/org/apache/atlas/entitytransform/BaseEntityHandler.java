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

import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.impexp.AttributeTransform;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.atlas.entitytransform.TransformationConstants.TYPE_NAME_ATTRIBUTE_NAME_SEP;

public class BaseEntityHandler {
    private static final Logger LOG = LoggerFactory.getLogger(BaseEntityHandler.class);

    protected final List<AtlasEntityTransformer> transformers;
    protected final boolean                      hasCustomAttributeTransformer;
    private TransformerContext                   transformerContext;

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
        if (!CollectionUtils.isNotEmpty(transformers)) {
            return entity;
        }

        AtlasTransformableEntity transformableEntity = getTransformableEntity(entity);
        if (transformableEntity == null) {
            return entity;
        }

        for (AtlasEntityTransformer transformer : transformers) {
            transformer.transform(transformableEntity);
        }

        transformableEntity.transformComplete();

        return entity;
    }

    private void setContextForActions(List<Action> actions) {
        for(Action action : actions) {
            if (action instanceof NeedsContext) {
                ((NeedsContext) action).setContext(transformerContext);
            }
        }
    }

    private void setContextForConditions(List<Condition> conditions) {
        for(Condition condition : conditions) {
            if (condition instanceof NeedsContext) {
                ((NeedsContext) condition).setContext(transformerContext);
            }
        }
    }

    public AtlasTransformableEntity getTransformableEntity(AtlasEntity entity) {
        return new AtlasTransformableEntity(entity);
    }

    public static List<BaseEntityHandler> createEntityHandlers(List<AttributeTransform> transforms, TransformerContext context) {
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
                handler.setContext(context);
            }
        }

        if (CollectionUtils.isEmpty(ret)) {
            BaseEntityHandler be = new BaseEntityHandler(transformers);
            be.setContext(context);

            ret.add(be);
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
    public void setContext(AtlasTypeRegistry typeRegistry, AtlasTypeDefStore typeDefStore, AtlasExportRequest request) {
        setContext(new TransformerContext(typeRegistry, typeDefStore, request));
    }

    public void setContext(TransformerContext context) {
        this.transformerContext = context;

        for (AtlasEntityTransformer transformer : transformers) {
            if (transformerContext != null) {
                setContextForActions(transformer.getActions());
                setContextForConditions(transformer.getConditions());
            }
        }
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

    public static List<BaseEntityHandler> fromJson(String transformersString, TransformerContext context) {
        if (StringUtils.isEmpty(transformersString)) {
            return null;
        }

        Object transformersObj = AtlasType.fromJson(transformersString, Object.class);
        List transformers = (transformersObj != null && transformersObj instanceof List) ? (List) transformersObj : null;

        List<AttributeTransform> attributeTransforms = new ArrayList<>();

        if (CollectionUtils.isEmpty(transformers)) {
            return null;
        }

        for (Object transformer : transformers) {
            String transformerStr = AtlasType.toJson(transformer);
            AttributeTransform attributeTransform = AtlasType.fromJson(transformerStr, AttributeTransform.class);

            if (attributeTransform == null) {
                continue;
            }

            attributeTransforms.add(attributeTransform);
        }

        if (CollectionUtils.isEmpty(attributeTransforms)) {
            return null;
        }

        List<BaseEntityHandler> entityHandlers = createEntityHandlers(attributeTransforms, context);
        if (CollectionUtils.isEmpty(entityHandlers)) {
            return null;
        }

        return entityHandlers;
    }
}