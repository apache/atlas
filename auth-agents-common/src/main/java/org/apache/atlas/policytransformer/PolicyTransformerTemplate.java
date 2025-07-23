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

import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.type.AtlasType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_SERVICE_NAME_ABAC;

public class PolicyTransformerTemplate {
    private static final Logger LOG = LoggerFactory.getLogger(PolicyTransformerTemplate.class);
    private static final String ABAC_TEMPLATE_KEY_SUFFIX = "abac";

    private Map<String, List<TemplatePolicy>> actionToPoliciesMap = new HashMap<>();

    public PolicyTransformerTemplate() {
    }

    public List<TemplatePolicy> getTemplate(String action) {
        return actionToPoliciesMap.get(action);
    }

    public Set<String> getTemplateActions() {
        return new HashSet<>(actionToPoliciesMap.keySet());
    }

    public String getTemplateKey(String action, String service) {
        if (POLICY_SERVICE_NAME_ABAC.equals(service)) {
            return action + "-" + ABAC_TEMPLATE_KEY_SUFFIX;
        }
        return action;
    }

    public void fromJsonString(String json) {

        Map<String, List<Map>> templates = AtlasType.fromJson(json, Map.class);

        for (String customAction : templates.keySet()) {
            List<Map> templatePolicies = templates.get(customAction);
            List<TemplatePolicy> policies = new ArrayList<>();

            for (Map policy: templatePolicies) {
                TemplatePolicy templatePolicy = new TemplatePolicy();

                templatePolicy.setActions((List<String>) policy.get("actions"));
                templatePolicy.setResources((List<String>) policy.get("resources"));
                templatePolicy.setPolicyType((String) policy.get("policyType"));
                templatePolicy.setPolicyResourceCategory((String) policy.get("policyResourceCategory"));
                templatePolicy.setPolicyServiceName((String) policy.get("policyServiceName"));
                templatePolicy.setPolicyConditions((List<Map>) policy.get("policyConditions"));

                Object filterCriteria = policy.get("policyFilterCriteria");
                if (filterCriteria != null) {
                    templatePolicy.setPolicyFilterCriteria((String) filterCriteria);
                }

                policies.add(templatePolicy);
            }

            this.actionToPoliciesMap.put(customAction, policies);
        }
    }

    class TemplatePolicy {
        private String policyServiceName;
        private String policyType;
        private List<String> resources;
        private List<String> actions;
        private String policyResourceCategory;
        private List<AtlasStruct> policyConditions = new ArrayList<>();
        private String policyFilterCriteria;

        public String getPolicyServiceName() {
            return policyServiceName;
        }

        public void setPolicyServiceName(String policyServiceName) {
            this.policyServiceName = policyServiceName;
        }

        public String getPolicyType() {
            return policyType;
        }

        public void setPolicyType(String policyType) {
            this.policyType = policyType;
        }

        public String getPolicyResourceCategory() {
            return policyResourceCategory;
        }

        public void setPolicyResourceCategory(String category) {
            this.policyResourceCategory = category;
        }

        public List<String> getResources() {
            return resources;
        }

        public void setResources(List<String> resources) {
            this.resources = resources;
        }

        public List<String> getActions() {
            return actions;
        }

        public void setActions(List<String> actions) {
            this.actions = actions;
        }

        public List<AtlasStruct> getPolicyConditions() {
            return policyConditions;
        }

        public void setPolicyConditions(List<Map> policyConditions) {
            if (policyConditions != null) {
                for (Map condition: policyConditions) {
                    this.policyConditions.add(new AtlasStruct(condition));
                }
            }
        }

        public String getPolicyFilterCriteria() {
            return policyFilterCriteria;
        }

        public void setPolicyFilterCriteria(String policyFilterCriteria) {
            this.policyFilterCriteria = policyFilterCriteria;
        }
    }
}