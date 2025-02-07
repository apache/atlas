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
package org.apache.atlas.utils;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.annotation.ConditionalOnAtlasProperty;
import org.apache.atlas.annotation.EnableConditional;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.core.type.AnnotationMetadata;

import java.util.Map;

public class OnAtlasEnableCondition implements Condition {
    private final Logger LOG = LoggerFactory.getLogger(OnAtlasEnableCondition.class);

    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        boolean matches = false;
        Map<String, Object> attributes = metadata.getAnnotationAttributes(EnableConditional.class.getName());
        String propertyName = (String) attributes.get("property");
        if (metadata instanceof AnnotatedTypeMetadata) {

            try {
                Configuration configuration = ApplicationProperties.get();
                boolean enabled = configuration.getBoolean(propertyName, (boolean) attributes.get("isDefault"));
                return enabled;
            } catch (AtlasException e) {
                LOG.error("Unable to load atlas properties. Dependent bean configuration may fail");
            }
        }
        return false;
    }
}