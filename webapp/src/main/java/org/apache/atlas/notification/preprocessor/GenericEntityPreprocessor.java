/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.notification.preprocessor;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class GenericEntityPreprocessor extends EntityPreprocessor {
    private final List<Pattern> entitiesToIgnore;
    private final List<Pattern> entityTypesToIgnore;

    public GenericEntityPreprocessor(List<Pattern> entityTypesToIgnore, List<Pattern> entitiesToIgnore) {
        super("Generic");

        this.entityTypesToIgnore = entityTypesToIgnore;
        this.entitiesToIgnore    = entitiesToIgnore;
    }

    @Override
    public void preprocess(AtlasEntity entity, PreprocessorContext context) {
        if (entity == null) {
            return;
        }

        if (isToBeIgnored(entity)) {
            context.addToIgnoredEntities(entity);
            return;
        }

        Object inputs  = entity.getAttribute(ATTRIBUTE_INPUTS);
        Object outputs = entity.getAttribute(ATTRIBUTE_OUTPUTS);

        filterProcessRelatedEntities(inputs, context);
        filterProcessRelatedEntities(outputs, context);

        filterRelationshipAttributes(entity.getRelationshipAttributes(), context);
    }

    private boolean isMatch(String property, List<Pattern> patterns) {
        return patterns.stream().anyMatch((Pattern pattern) -> pattern.matcher(property).matches());
    }

    private void filterProcessRelatedEntities(Object obj, PreprocessorContext context) {
        if (obj == null || !(obj instanceof Collection)) {
            return;
        }

        Collection objList  = (Collection) obj;
        List       toRemove = new ArrayList();

        for (Object entity : objList) {
            if (isToBeIgnored(entity)) {
                context.addToIgnoredEntities(entity);
                toRemove.add(entity);
            }
        }

        objList.removeAll(toRemove);
    }

    private boolean isToBeIgnored(Object entity) {
        String  qualifiedName = getQualifiedName(entity);
        String  typeName      = getTypeName(entity);
        boolean decision      = false;

        if (qualifiedName != null && typeName != null) {
            if (CollectionUtils.isEmpty(this.entityTypesToIgnore)) { // Will Ignore all entities whose qualified name matches the ignore pattern.
                decision = isMatch(qualifiedName, this.entitiesToIgnore);
            } else if (CollectionUtils.isEmpty(this.entitiesToIgnore)) { // Will Ignore all entities whose type matches the regex given.
                decision = isMatch(typeName, this.entityTypesToIgnore);
            } else { // Combination of above 2 cases.
                decision = isMatch(typeName, this.entityTypesToIgnore) && isMatch(qualifiedName, this.entitiesToIgnore);
            }
        }

        return decision;
    }

    private void filterRelationshipAttributes(Map<String, Object> relationshipAttributes, PreprocessorContext context) {
        if (relationshipAttributes == null) {
            return;
        }
        List<String> keysToRemove = new ArrayList<>();

        for (Map.Entry<String, Object> entry : relationshipAttributes.entrySet()) {
            Object obj = entry.getValue();

            if (obj instanceof Collection) {
                Collection entities = (Collection) obj;

                entities.removeIf((Object entity) -> {
                    if (isToBeIgnored(entity)) {
                        context.addToIgnoredEntities(entity);
                        return true;
                    }
                    return false;
                });
            } else {
                if (isToBeIgnored(obj)) {
                    context.addToIgnoredEntities(obj);
                    keysToRemove.add(entry.getKey());
                }
            }
        }
        for (String key : keysToRemove) {
            relationshipAttributes.remove(key);
        }
    }
}
