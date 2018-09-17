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
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class Condition {
    private static final Logger LOG = LoggerFactory.getLogger(Condition.class);

    private static final String CONDITION_DELIMITER                    = ":";
    private static final String CONDITION_NAME_EQUALS                  = "EQUALS";
    private static final String CONDITION_NAME_EQUALS_IGNORE_CASE      = "EQUALS_IGNORE_CASE";
    private static final String CONDITION_NAME_STARTS_WITH             = "STARTS_WITH";
    private static final String CONDITION_NAME_STARTS_WITH_IGNORE_CASE = "STARTS_WITH_IGNORE_CASE";

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
}
