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
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.atlas.policytransformer.CachePolicyTransformerImpl.ATTR_POLICY_RESOURCES;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_ACTIONS;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_IS_ENABLED;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_RESOURCES_CATEGORY;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_SERVICE_NAME;
import static org.apache.atlas.repository.util.AccessControlUtils.RESOURCES_TAG;
import static org.apache.atlas.repository.util.AccessControlUtils.getFilteredPolicyResources;
import static org.apache.atlas.repository.util.AccessControlUtils.getIsPolicyEnabled;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyActions;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyResources;

public class PurposeCachePolicyTransformer extends AbstractCachePolicyTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PurposeCachePolicyTransformer.class);

    private final static String TEMPLATE_SUFFIX = "purpose";

    private EntityGraphRetriever entityRetriever = null;
    private PolicyTransformerTemplate purposeTemplate;

    public PurposeCachePolicyTransformer(EntityGraphRetriever entityRetriever) throws AtlasBaseException {
        purposeTemplate = getTemplate(TEMPLATE_SUFFIX);
        this.entityRetriever = entityRetriever;
    }

    public List<AtlasEntityHeader> transform(AtlasEntityHeader atlasPolicy) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("PurposeCachePolicyTransformer.transform");
        List<AtlasEntityHeader> ret = new ArrayList<>();

        List<String> atlasActions = getPolicyActions(atlasPolicy);
        List<String> atlasResources = getPolicyResources(atlasPolicy);
        List<String> tags = getFilteredPolicyResources(atlasResources, RESOURCES_TAG);

        int index = 0;
        Set<String> templateActions = purposeTemplate.getTemplateActions();
        List<String> transformableActions = (List<String>) CollectionUtils.intersection(atlasActions, templateActions);


        for (String atlasAction : transformableActions) {
            List<PolicyTransformerTemplate.TemplatePolicy> currentTemplates = purposeTemplate.getTemplate(atlasAction);

            if (CollectionUtils.isEmpty(currentTemplates)) {
                LOG.warn("PurposeCachePolicyTransformer: Skipping unknown action {} while transforming policy {}", atlasAction, atlasPolicy.getGuid());
                continue;
            }

            for (PolicyTransformerTemplate.TemplatePolicy templatePolicy : currentTemplates) {
                AtlasEntityHeader header = new AtlasEntityHeader(atlasPolicy);

                header.setGuid(atlasPolicy.getGuid() + "-" + index++);

                header.setAttribute(ATTR_POLICY_ACTIONS, templatePolicy.getActions());
                header.setAttribute(ATTR_POLICY_RESOURCES_CATEGORY, templatePolicy.getPolicyResourceCategory());
                header.setAttribute(ATTR_POLICY_IS_ENABLED, getIsPolicyEnabled(atlasPolicy));

                if (StringUtils.isNotEmpty(templatePolicy.getPolicyServiceName())) {
                    header.setAttribute(ATTR_POLICY_SERVICE_NAME, templatePolicy.getPolicyServiceName());
                }

                List<String> finalResources = new ArrayList<>();

                for (String templateResource : templatePolicy.getResources()) {
                    if (templateResource.contains(PLACEHOLDER_TAG)) {
                        tags.forEach(tag -> finalResources.add(templateResource.replace(PLACEHOLDER_TAG, tag)));
                    } else {
                        finalResources.add(templateResource);
                    }
                }
                header.setAttribute(ATTR_POLICY_RESOURCES, finalResources);

                ret.add(header);
            }
        }

        //prepare a policy for all non-transformable actions
        //this will help to reduce number of overall transformed policies
        List<String> nonTransformableActions = (List<String>) CollectionUtils.subtract(atlasActions, templateActions);

        if (CollectionUtils.isNotEmpty(nonTransformableActions)) {
            AtlasEntityHeader header = new AtlasEntityHeader(atlasPolicy);
            header.setGuid(atlasPolicy.getGuid() + "-" + index);
            header.setAttribute(ATTR_POLICY_ACTIONS, nonTransformableActions);
            ret.add(header);
        }

        RequestContext.get().endMetricRecord(recorder);
        return ret;
    }
}
