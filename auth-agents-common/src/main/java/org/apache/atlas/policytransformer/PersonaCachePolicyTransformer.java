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

import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.atlas.policytransformer.CachePolicyTransformerImpl.ATTR_NAME;
import static org.apache.atlas.policytransformer.CachePolicyTransformerImpl.ATTR_POLICY_RESOURCES;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_ACTIONS;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_IS_ENABLED;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_RESOURCES_CATEGORY;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_SUB_CATEGORY_DATA;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_SUB_CATEGORY_METADATA;
import static org.apache.atlas.repository.util.AccessControlUtils.RESOURCES_ENTITY;
import static org.apache.atlas.repository.util.AccessControlUtils.RESOURCES_ENTITY_TYPE;
import static org.apache.atlas.repository.util.AccessControlUtils.getConnectionEntity;
import static org.apache.atlas.repository.util.AccessControlUtils.getFilteredPolicyResources;
import static org.apache.atlas.repository.util.AccessControlUtils.getIsPolicyEnabled;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyActions;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyAssets;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyResources;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicySubCategory;

public class PersonaCachePolicyTransformer extends AbstractCachePolicyTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PersonaCachePolicyTransformer.class);

    private EntityGraphRetriever entityRetriever = null;

    public PersonaCachePolicyTransformer(EntityGraphRetriever entityRetriever) throws AtlasBaseException {
        super();
        this.entityRetriever = entityRetriever;
    }

    public List<AtlasEntityHeader> transform(AtlasEntityHeader atlasPolicy) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("PersonaCachePolicyTransformer.transform");
        List<AtlasEntityHeader> ret = new ArrayList<>();

        List<String> atlasActions = getPolicyActions(atlasPolicy);
        List<String> atlasResources = getPolicyResources(atlasPolicy);
        List<String> entityResources = getFilteredPolicyResources(atlasResources, RESOURCES_ENTITY);
        List<String> typeResources = getFilteredPolicyResources(atlasResources, RESOURCES_ENTITY_TYPE);

        int index = 0;
        for (String atlasAction : atlasActions) {
            List<PolicyTransformerTemplate.TemplatePolicy> currentTemplates = templates.getTemplate(atlasAction);

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
                                isConnection = isConnectionPolicy(entityResources);
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

                header.setAttribute(ATTR_NAME, "transformed_policy_persona");

                ret.add(header);
            }
        }

        RequestContext.get().endMetricRecord(recorder);
        return ret;
    }

    private boolean isConnectionPolicy(List<String> assets) {

        if (assets.size() == 1) {
            AtlasEntity connection;
            try {
                connection = getConnectionEntity(entityRetriever, assets.get(0));
            } catch (AtlasBaseException abe) {
                return false;
            }
            return connection != null;
        }

        return false;
    }
}
