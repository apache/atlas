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
package org.apache.atlas.transformer;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.repository.store.graph.v2.preprocessor.ConnectionPreProcessor;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.repository.Constants.NAME;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.Constants.getStaticFileAsString;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_RESOURCES;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_ROLES;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyRoles;
import static org.apache.atlas.repository.util.AtlasEntityUtils.getListAttribute;
import static org.apache.atlas.repository.util.AtlasEntityUtils.getName;
import static org.apache.atlas.repository.util.AtlasEntityUtils.getQualifiedName;

public class PreProcessorPoliciesTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionPreProcessor.class);

    static final String PLACEHOLDER_ENTITY = "{entity}";
    static final String PLACEHOLDER_GUID = "{guid}";
    static final String PLACEHOLDER_NAME = "{name}";

    public PreProcessorPoliciesTransformer() {

    }

    public AtlasEntitiesWithExtInfo transform(AtlasEntity entity) throws AtlasBaseException {
        LOG.info("transforming preprocessor bootstrap policies");
        String qualifiedName = getQualifiedName(entity);
        String guid = entity.getGuid();
        String name = getName(entity);

        AtlasEntitiesWithExtInfo policiesExtInfo = new AtlasEntitiesWithExtInfo();

        for (AtlasEntity templatePolicy : TemplateHelper.getTemplate(entity.getTypeName())) {
            AtlasEntity policy = new AtlasEntity(templatePolicy);
            String bootPolicyName = getName(policy);
            String bootPolicyQn = getQualifiedName(policy);

            policy.setAttribute(NAME, bootPolicyName.replace(PLACEHOLDER_NAME, name));
            policy.setAttribute(QUALIFIED_NAME, bootPolicyQn.replace(PLACEHOLDER_GUID, guid));

            List<String> policyRoles = getPolicyRoles(policy);
            List<String> finalRoles = new ArrayList<>();
            for (String role : policyRoles) {
                finalRoles.add(role.replace(PLACEHOLDER_GUID, guid));
            }
            policy.setAttribute(ATTR_POLICY_ROLES, finalRoles);

            List<String> resources = getListAttribute(policy, ATTR_POLICY_RESOURCES);
            List<String> resourcesFinal  = new ArrayList<>();
            for (String resource : resources) {
                if (resource.contains(PLACEHOLDER_ENTITY)) {
                    resource = resource.replace(PLACEHOLDER_ENTITY, qualifiedName);
                }

                resourcesFinal.add(resource);
            }
            policy.setAttribute(ATTR_POLICY_RESOURCES, resourcesFinal);

            policiesExtInfo.addEntity(policy);
        }
        LOG.info("transformed preprocessor bootstrap policies");

        return policiesExtInfo;
    }

    static class TemplateHelper {
        static final String TEMPLATE_FILE_NAME_PATTERN = "templates/%s_bootstrap_policies.json";

        private static Map<String, List<AtlasEntity>> templates = new HashMap<>();

        public static List<AtlasEntity> getTemplate(String entityTypeName) throws AtlasBaseException {
            if (StringUtils.isEmpty(entityTypeName)) {
                throw new AtlasBaseException("Please provide entityTypeName to fetch template");
            }

            if (MapUtils.isEmpty(templates) || !templates.containsKey(entityTypeName)) {
                templates.put(entityTypeName, loadTemplate(entityTypeName));
            }

            return templates.get(entityTypeName);
        }

        private static List<AtlasEntity> loadTemplate(String entityTypeName) {
            String jsonTemplate = null;
            String templateName = String.format(TEMPLATE_FILE_NAME_PATTERN, entityTypeName.toLowerCase());

            try {
                jsonTemplate = getStaticFileAsString(templateName);
            } catch (IOException e) {
                LOG.error("Failed to load template for policies: {}", templateName);
            }

            AtlasEntity[] entities = AtlasType.fromJson(jsonTemplate, AtlasEntity[].class);
            return Arrays.asList(entities);
        }
    }
}
