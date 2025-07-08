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
package org.apache.atlas.policytransformer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.atlas.RequestContext;
import org.apache.atlas.authorizer.JsonToElasticsearchQuery;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.atlas.policytransformer.CachePolicyTransformerImpl.ATTR_NAME;
import static org.apache.atlas.policytransformer.CachePolicyTransformerImpl.ATTR_POLICY_RESOURCES;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_ACTIONS;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_FILTER_CRITERIA;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_IS_ENABLED;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_RESOURCES_CATEGORY;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_SERVICE_NAME_ABAC;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_SUB_CATEGORY_DATA;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_SUB_CATEGORY_METADATA;
import static org.apache.atlas.repository.util.AccessControlUtils.RESOURCES_ENTITY;
import static org.apache.atlas.repository.util.AccessControlUtils.RESOURCES_ENTITY_TYPE;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_CONDITIONS;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_ENTITY;
import static org.apache.atlas.repository.util.AccessControlUtils.getEntityByQualifiedName;
import static org.apache.atlas.repository.util.AccessControlUtils.getFilteredPolicyResources;
import static org.apache.atlas.repository.util.AccessControlUtils.getIsPolicyEnabled;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyActions;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyConnectionQN;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyFilterCriteria;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyResources;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyServiceName;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicySubCategory;

public class PersonaCachePolicyTransformer extends AbstractCachePolicyTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PersonaCachePolicyTransformer.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private final static String TEMPLATE_SUFFIX = "persona";

    private EntityGraphRetriever entityRetriever = null;
    private PolicyTransformerTemplate personaTemplate;

    public PersonaCachePolicyTransformer(EntityGraphRetriever entityRetriever) throws AtlasBaseException {
        personaTemplate = getTemplate(TEMPLATE_SUFFIX);
        this.entityRetriever = entityRetriever;
    }

    public List<AtlasEntityHeader> transform(AtlasEntityHeader atlasPolicy) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("PersonaCachePolicyTransformer.transform");
        List<AtlasEntityHeader> ret = new ArrayList<>();

        List<String> atlasActions = getPolicyActions(atlasPolicy);
        List<String> atlasResources = getPolicyResources(atlasPolicy);
        List<String> entityResources = getFilteredPolicyResources(atlasResources, RESOURCES_ENTITY);
        List<String> typeResources = getFilteredPolicyResources(atlasResources, RESOURCES_ENTITY_TYPE);

        String policyServiceName = getPolicyServiceName(atlasPolicy);
        String policyFilterCriteria = getPolicyFilterCriteria(atlasPolicy);

        int index = 0;
        for (String atlasAction : atlasActions) {
            List<PolicyTransformerTemplate.TemplatePolicy> currentTemplates = personaTemplate.getTemplate(personaTemplate.getTemplateKey(atlasAction, policyServiceName));

            if (CollectionUtils.isEmpty(currentTemplates)) {
                LOG.warn("PolicyTransformerImpl: Skipping unknown action {} while transforming policy {}", atlasAction, atlasPolicy.getGuid());
                continue;
            }

            for (PolicyTransformerTemplate.TemplatePolicy templatePolicy : currentTemplates) {
                AtlasEntityHeader header = new AtlasEntityHeader(atlasPolicy);

                header.setGuid(atlasPolicy.getGuid() + "-" + index++);

                header.setAttribute(ATTR_POLICY_ACTIONS, templatePolicy.getActions());
                header.setAttribute(ATTR_POLICY_RESOURCES_CATEGORY, templatePolicy.getPolicyResourceCategory());
                header.setAttribute(ATTR_POLICY_IS_ENABLED, getIsPolicyEnabled(atlasPolicy));
                header.setAttribute(ATTR_NAME, "transformed_policy_persona");

                header.setAttribute(ATTR_POLICY_CONDITIONS, buildPolicyConditions(atlasPolicy, templatePolicy));

                if (POLICY_SERVICE_NAME_ABAC.equals(policyServiceName)) {
                    String templateFilterCriteria = templatePolicy.getPolicyFilterCriteria();
                    JsonNode entityCriteria = JsonToElasticsearchQuery.parseFilterJSON(policyFilterCriteria, POLICY_FILTER_CRITERIA_ENTITY);
                    if (entityCriteria != null && StringUtils.isNotEmpty(templateFilterCriteria)) {
                        templateFilterCriteria = templateFilterCriteria.replace(PLACEHOLDER_FILTER_CRITERIA, policyFilterCriteria);
                        templateFilterCriteria = templateFilterCriteria.replace(PLACEHOLDER_FILTER_ENTITY_CRITERIA, entityCriteria.toString());
                        header.setAttribute(ATTR_POLICY_FILTER_CRITERIA, templateFilterCriteria);
                        header.setAttribute(ATTR_POLICY_RESOURCES, new ArrayList<>(0));
                    }
                } else {
                    String subCategory = getPolicySubCategory(atlasPolicy);

                    List<String> finalResources = new ArrayList<>();

                    for (String templateResource : templatePolicy.getResources()) {
                        if (templateResource.contains(PLACEHOLDER_ENTITY)) {
                            for (String entityResource : entityResources) {
                                finalResources.add(templateResource.replace(PLACEHOLDER_ENTITY, entityResource));
                            }

                        } else if (templateResource.contains(PLACEHOLDER_ENTITY_TYPE)) {

                            if (CollectionUtils.isNotEmpty(typeResources)) {
                                typeResources.forEach(x -> finalResources.add(templateResource.replace(PLACEHOLDER_ENTITY_TYPE, x)));
                            } else {
                                boolean isConnection = false;

                                if (POLICY_SUB_CATEGORY_METADATA.equals(subCategory) || POLICY_SUB_CATEGORY_DATA.equals(subCategory)) {
                                    isConnection = isConnectionPolicy(entityResources, atlasPolicy);
                                }

                                if (isConnection) {
                                    finalResources.add(templateResource.replace(PLACEHOLDER_ENTITY_TYPE, "*"));
                                } else {
                                    finalResources.add(templateResource.replace(PLACEHOLDER_ENTITY_TYPE, "Process"));
                                    finalResources.add(templateResource.replace(PLACEHOLDER_ENTITY_TYPE, "Catalog"));
                                }
                            }
                        } else {
                            finalResources.add(templateResource);
                        }
                    }
                    header.setAttribute(ATTR_POLICY_RESOURCES, finalResources);
                }
                ret.add(header);
            }
        }

        RequestContext.get().endMetricRecord(recorder);
        return ret;
    }

    private boolean isConnectionPolicy(List<String> assets, AtlasEntityHeader atlasPolicy) {

        if (assets.size() == 1) {
            String connQNAttr = getPolicyConnectionQN(atlasPolicy);

            if (StringUtils.isNotEmpty(connQNAttr)) {
                return connQNAttr.equals(assets.get(0));
            } else {
                AtlasEntity connection;
                try {
                    connection = getEntityByQualifiedName(entityRetriever, assets.get(0));
                } catch (AtlasBaseException abe) {
                    return false;
                }
                return connection != null;
            }
        }

        return false;
    }

    private List<AtlasStruct> buildPolicyConditions(AtlasEntityHeader atlasPolicy, PolicyTransformerTemplate.TemplatePolicy templatePolicy) {
        List<AtlasStruct> combinedConditions = new ArrayList<>();

        try {
            List<AtlasStruct> atlasConditions = (List<AtlasStruct>) atlasPolicy.getAttribute(ATTR_POLICY_CONDITIONS);
            if (CollectionUtils.isNotEmpty(atlasConditions)) {
                combinedConditions.addAll(atlasConditions);
            }

            List<AtlasStruct> templateConditions = templatePolicy.getPolicyConditions();
            if (CollectionUtils.isNotEmpty(templateConditions)) {
                combinedConditions.addAll(templateConditions);
            }

        } catch (Exception e) {
            LOG.warn("Error processing policy conditions: {}", e.getMessage());
            LOG.warn("Exception while processing policy conditions", e);
        }

        return combinedConditions;
    }
}