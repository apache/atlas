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

import org.apache.atlas.entitytransform.BaseEntityHandler.AtlasTransformableEntity;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;


public abstract class Condition {
    private static final Logger LOG = LoggerFactory.getLogger(Condition.class);

    private static final String CONDITION_DELIMITER                    = ":";
    private static final String CONDITION_ENTITY_OBJECT_ID             = "OBJECTID";
    private static final String CONDITION_ENTITY_TOP_LEVEL             = "TOPLEVEL";
    private static final String CONDITION_ENTITY_ALL                   = "ALL";
    private static final String CONDITION_NAME_EQUALS                  = "EQUALS";
    private static final String CONDITION_NAME_EQUALS_IGNORE_CASE      = "EQUALS_IGNORE_CASE";
    private static final String CONDITION_NAME_STARTS_WITH             = "STARTS_WITH";
    private static final String CONDITION_NAME_STARTS_WITH_IGNORE_CASE = "STARTS_WITH_IGNORE_CASE";
    private static final String CONDITION_NAME_HAS_VALUE               = "HAS_VALUE";

    protected final String attributeName;

    protected Condition(String attributeName) {
        this.attributeName = attributeName;
    }

    public String getAttributeName() { return attributeName; }

    public abstract boolean matches(AtlasTransformableEntity entity);


    public static Condition createCondition(String key, String value) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> Condition.createCondition(key={}, value={})", key, value);
        }

        final Condition ret;

        int    idxConditionDelim = value == null ? -1 : value.indexOf(CONDITION_DELIMITER);
        String conditionName     = idxConditionDelim == -1 ? CONDITION_NAME_EQUALS : value.substring(0, idxConditionDelim);
        String conditionValue    = idxConditionDelim == -1 ? value : value.substring(idxConditionDelim + CONDITION_DELIMITER.length());

        conditionName  = StringUtils.trim(conditionName);
        conditionValue = StringUtils.trim(conditionValue);
        value          = StringUtils.trim(value);

        switch (conditionName.toUpperCase()) {
            case CONDITION_ENTITY_ALL:
                ret = new ObjectIdEquals(key, CONDITION_ENTITY_ALL);
                break;

            case CONDITION_ENTITY_TOP_LEVEL:
                ret = new ObjectIdEquals(key, CONDITION_ENTITY_TOP_LEVEL);
                break;

            case CONDITION_ENTITY_OBJECT_ID:
                ret = new ObjectIdEquals(key, conditionValue);
                break;

            case CONDITION_NAME_EQUALS:
                ret = new EqualsCondition(key, conditionValue);
            break;

            case CONDITION_NAME_EQUALS_IGNORE_CASE:
                ret = new EqualsIgnoreCaseCondition(key, conditionValue);
            break;

            case CONDITION_NAME_STARTS_WITH:
                ret = new StartsWithCondition(key, conditionValue);
            break;

            case CONDITION_NAME_STARTS_WITH_IGNORE_CASE:
                ret = new StartsWithIgnoreCaseCondition(key, conditionValue);
            break;

            case CONDITION_NAME_HAS_VALUE:
                ret = new HasValueCondition(key, conditionValue);
                break;

            default:
                ret = new EqualsCondition(key, value); // treat unspecified/unknown condition as 'EQUALS'
            break;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== Condition.createCondition(key={}, value={}): actionName={}, actionValue={}, ret={}", key, value, conditionName, conditionValue, ret);
        }

        return ret;
    }


    public static class EqualsCondition extends Condition {
        protected final String attributeValue;

        public EqualsCondition(String attributeName, String attributeValue) {
            super(attributeName);

            this.attributeValue = attributeValue;
        }

        @Override
        public boolean matches(AtlasTransformableEntity entity) {
            Object attributeValue = entity != null ? entity.getAttribute(attributeName) : null;

            return attributeValue != null && StringUtils.equals(attributeValue.toString(), this.attributeValue);
        }
    }


    public static class EqualsIgnoreCaseCondition extends Condition {
        protected final String attributeValue;

        public EqualsIgnoreCaseCondition(String attributeName, String attributeValue) {
            super(attributeName);

            this.attributeValue = attributeValue;
        }

        @Override
        public boolean matches(AtlasTransformableEntity entity) {
            Object attributeValue = entity != null ? entity.getAttribute(attributeName) : null;

            return attributeValue != null && StringUtils.equalsIgnoreCase(attributeValue.toString(), this.attributeValue);
        }
    }


    public static class StartsWithCondition extends Condition {
        protected final String prefix;

        public StartsWithCondition(String attributeName, String prefix) {
            super(attributeName);

            this.prefix = prefix;
        }

        @Override
        public boolean matches(AtlasTransformableEntity entity) {
            Object attributeValue = entity != null ? entity.getAttribute(attributeName) : null;

            return attributeValue != null && StringUtils.startsWith(attributeValue.toString(), this.prefix);
        }
    }


    public static class StartsWithIgnoreCaseCondition extends Condition {
        protected final String prefix;

        public StartsWithIgnoreCaseCondition(String attributeName, String prefix) {
            super(attributeName);

            this.prefix = prefix;
        }

        @Override
        public boolean matches(AtlasTransformableEntity entity) {
            Object attributeValue = entity != null ? entity.getAttribute(attributeName) : null;

            return attributeValue != null && StringUtils.startsWithIgnoreCase(attributeValue.toString(), this.prefix);
        }
    }

    static class ObjectIdEquals extends Condition implements NeedsContext {
        private final List<AtlasObjectId> objectIds;
        private String scope;
        private TransformerContext transformerContext;

        public ObjectIdEquals(String key, String conditionValue) {
            super(key);

            objectIds = new ArrayList<>();
            this.scope = conditionValue;
        }

        @Override
        public boolean matches(AtlasTransformableEntity entity) {
            for (AtlasObjectId objectId : objectIds) {
                return isMatch(objectId, entity.entity);
            }

            return objectIds.size() == 0;
        }

        public void add(AtlasObjectId objectId) {
            this.objectIds.add(objectId);
        }

        private boolean isMatch(AtlasObjectId objectId, AtlasEntity entity) {
            boolean ret = true;
            if (!StringUtils.isEmpty(objectId.getGuid())) {
                return Objects.equals(objectId.getGuid(), entity.getGuid());
            }

            ret = Objects.equals(objectId.getTypeName(), entity.getTypeName());
            if (!ret) {
                return ret;
            }

            for (Map.Entry<String, Object> entry : objectId.getUniqueAttributes().entrySet()) {
                ret = ret && Objects.equals(entity.getAttribute(entry.getKey()), entry.getValue());
                if (!ret) {
                    break;
                }
            }

            return ret;
        }

        @Override
        public void setContext(TransformerContext transformerContext) {
            this.transformerContext = transformerContext;
            if(StringUtils.isEmpty(scope) || scope.equals(CONDITION_ENTITY_ALL)) {
                return;
            }

            addObjectIdsFromExportRequest();
        }

        private void addObjectIdsFromExportRequest() {
            for(AtlasObjectId objectId : this.transformerContext.getExportRequest().getItemsToExport()) {
                add(objectId);
            }
        }
    }


    public static class HasValueCondition extends Condition {
        protected final String attributeValue;

        public HasValueCondition(String attributeName, String attributeValue) {
            super(attributeName);

            this.attributeValue = attributeValue;
        }

        @Override
        public boolean matches(AtlasTransformableEntity entity) {
            Object attributeValue = entity != null ? entity.getAttribute(attributeName) : null;

            return attributeValue != null ? StringUtils.isNotEmpty(attributeValue.toString()) : false;
        }
    }
}
