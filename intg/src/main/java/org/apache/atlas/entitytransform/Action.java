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

import org.apache.commons.lang.StringUtils;
import org.apache.atlas.entitytransform.BaseEntityHandler.AtlasTransformableEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class Action {
    private static final Logger LOG = LoggerFactory.getLogger(Action.class);

    private static final String ACTION_DELIMITER           = ":";
    private static final String ACTION_NAME_SET            = "SET";
    private static final String ACTION_NAME_REPLACE_PREFIX = "REPLACE_PREFIX";
    private static final String ACTION_NAME_TO_LOWER       = "TO_LOWER";
    private static final String ACTION_NAME_TO_UPPER       = "TO_UPPER";
    private static final String ACTION_NAME_CLEAR          = "CLEAR";

    protected final String attributeName;


    protected Action(String attributeName) {
        this.attributeName = attributeName;
    }

    public String getAttributeName() { return attributeName; }

    public boolean isValid() {
        return StringUtils.isNotEmpty(attributeName);
    }

    public abstract void apply(AtlasTransformableEntity entity);


    public static Action createAction(String key, String value) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> Action.createAction(key={}, value={})", key, value);
        }

        final Action ret;

        int    idxActionDelim = value == null ? -1 : value.indexOf(ACTION_DELIMITER);
        String actionName     = idxActionDelim == -1 ? ACTION_NAME_SET : value.substring(0, idxActionDelim);
        String actionValue    = idxActionDelim == -1 ? value : value.substring(idxActionDelim + ACTION_DELIMITER.length());

        actionName  = StringUtils.trim(actionName);
        actionValue = StringUtils.trim(actionValue);
        value       = StringUtils.trim(value);

        switch (actionName.toUpperCase()) {
            case ACTION_NAME_REPLACE_PREFIX:
                ret = new PrefixReplaceAction(key, actionValue);
            break;

            case ACTION_NAME_TO_LOWER:
                ret = new ToLowerCaseAction(key);
            break;

            case ACTION_NAME_TO_UPPER:
                ret = new ToUpperCaseAction(key);
            break;

            case ACTION_NAME_SET:
                ret = new SetAction(key, actionValue);
            break;

            case ACTION_NAME_CLEAR:
                ret = new ClearAction(key);
                break;

            default:
                ret = new SetAction(key, value); // treat unspecified/unknown action as 'SET'
            break;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== Action.createAction(key={}, value={}): actionName={}, actionValue={}, ret={}", key, value, actionName, actionValue, ret);
        }

        return ret;
    }


    public static class SetAction extends Action {
        private final String attributeValue;

        public SetAction(String attributeName, String attributeValue) {
            super(attributeName);

            this.attributeValue = attributeValue;
        }

        @Override
        public void apply(AtlasTransformableEntity entity) {
            if (isValid()) {
                entity.setAttribute(attributeName, attributeValue);
            }
        }
    }


    public static class PrefixReplaceAction extends Action {
        private final String fromPrefix;
        private final String toPrefix;

        public PrefixReplaceAction(String attributeName, String actionValue) {
            super(attributeName);

            // actionValue => =:prefixToReplace=replacedValue
            if (actionValue != null) {
                int idxSepDelimiter = actionValue.indexOf(ACTION_DELIMITER);

                if (idxSepDelimiter == -1) { // no separator specified i.e. no value specified to replace; remove the prefix
                    fromPrefix = actionValue;
                    toPrefix   = "";
                } else {
                    String prefixSep    = StringUtils.trim(actionValue.substring(0, idxSepDelimiter));
                    int    idxPrefixSep = actionValue.indexOf(prefixSep, idxSepDelimiter + ACTION_DELIMITER.length());

                    if (idxPrefixSep == -1) { // separator not found i.e. no value specified to replace; remove the prefix
                        fromPrefix = actionValue.substring(idxSepDelimiter + ACTION_DELIMITER.length());
                        toPrefix   = "";
                    } else {
                        fromPrefix = actionValue.substring(idxSepDelimiter + ACTION_DELIMITER.length(), idxPrefixSep);
                        toPrefix   = actionValue.substring(idxPrefixSep + prefixSep.length());
                    }
                }
            } else {
                fromPrefix = null;
                toPrefix   = "";
            }
        }

        @Override
        public boolean isValid() {
            return super.isValid() && StringUtils.isNotEmpty(fromPrefix);
        }

        @Override
        public void apply(AtlasTransformableEntity entity) {
            if (isValid()) {
                Object currValue = entity.getAttribute(attributeName);
                String strValue  = currValue != null ? currValue.toString() : null;

                if (strValue != null && strValue.startsWith(fromPrefix)) {
                    entity.setAttribute(attributeName, StringUtils.replace(strValue, fromPrefix, toPrefix, 1));
                }
            }
        }
    }

    public static class ToLowerCaseAction extends Action {
        public ToLowerCaseAction(String attributeName) {
            super(attributeName);
        }

        @Override
        public void apply(AtlasTransformableEntity entity) {
            if (isValid()) {
                Object currValue = entity.getAttribute(attributeName);
                String strValue  = currValue instanceof String ? (String) currValue : null;

                if (strValue != null) {
                    entity.setAttribute(attributeName, strValue.toLowerCase());
                }
            }
        }
    }

    public static class ToUpperCaseAction extends Action {
        public ToUpperCaseAction(String attributeName) {
            super(attributeName);
        }

        @Override
        public void apply(AtlasTransformableEntity entity) {
            if (isValid()) {
                Object currValue = entity.getAttribute(attributeName);
                String strValue  = currValue instanceof String ? (String) currValue : null;

                if (strValue != null) {
                    entity.setAttribute(attributeName, strValue.toUpperCase());
                }
            }
        }
    }

    public static class ClearAction extends Action {
        public ClearAction(String attributeName) {
            super(attributeName);
        }

        @Override
        public void apply(AtlasTransformableEntity entity) {
            if (isValid() && entity.hasAttribute(attributeName)) {
                entity.setAttribute(attributeName, null);
            }
        }
    }
}
