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

import java.util.List;
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
        if (entity != null && isToBeIgnored(entity)) {
            context.addToIgnoredEntities(entity);
        }
    }

    private boolean isMatch(String property, List<Pattern> patterns) {
        return patterns.stream().anyMatch((Pattern pattern) -> pattern.matcher(property).matches());
    }

    private boolean isToBeIgnored(AtlasEntity entity) {
        String  qualifiedName = getQualifiedName(entity);
        boolean decision;

        if (CollectionUtils.isEmpty(this.entityTypesToIgnore)) { // Will Ignore all entities whose qualified name matches the ignore pattern.
            decision = isMatch(qualifiedName, this.entitiesToIgnore);
        } else if (CollectionUtils.isEmpty(this.entitiesToIgnore)) { // Will Ignore all entities whose type matches the regex given.
            decision = isMatch(entity.getTypeName(), this.entityTypesToIgnore);
        } else { // Combination of above 2 cases.
            decision = isMatch(entity.getTypeName(), this.entityTypesToIgnore) && isMatch(qualifiedName, this.entitiesToIgnore);
        }

        return decision;
    }
}
