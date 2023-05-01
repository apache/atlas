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

import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.plugin.util.ServicePolicies;
import org.apache.atlas.plugin.model.RangerPolicy;
import org.apache.atlas.plugin.model.RangerPolicy.RangerDataMaskPolicyItem;
import org.apache.atlas.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.atlas.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.atlas.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.atlas.plugin.model.RangerPolicy.RangerPolicyItemDataMaskInfo;
import org.apache.atlas.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.atlas.plugin.model.RangerServiceDef;
import org.apache.atlas.plugin.model.RangerValiditySchedule;
import org.apache.atlas.plugin.util.ServicePolicies.TagPolicies;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusGraph;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.v1.model.instance.Id;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.Constants.NAME;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.Constants.SERVICE_ENTITY_TYPE;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_CATEGORY;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_IS_ENABLED;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_PRIORITY;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_SERVICE_NAME;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_SUB_CATEGORY;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_CATEGORY_PERSONA;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_CATEGORY_PURPOSE;
import static org.apache.atlas.repository.util.AccessControlUtils.getIsPolicyEnabled;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyCategory;

@Component
public class CachePolicyTransformerImpl {
    private static final Logger LOG = LoggerFactory.getLogger(CachePolicyTransformerImpl.class);

    private static final String RESOURCES_SPLITTER = ":";

    static final String ATTR_QUALIFIED_NAME = "qualifiedName";
    static final String ATTR_NAME           = "name";

    public static final String ATTR_POLICY_ACTIONS            = "policyActions";
    public static final String ATTR_POLICY_TYPE               = "policyType";
    public static final String ATTR_POLICY_RESOURCES          = "policyResources";

    public static final String ATTR_SERVICE_SERVICE_TYPE = "authServiceType";
    public static final String ATTR_SERVICE_TAG_SERVICE  = "tagService";
    public static final String ATTR_SERVICE_IS_ENABLED   = "authServiceIsEnabled";
    public static final String ATTR_SERVICE_LAST_SYNC    = "authServicePolicyLastSync";

    private static final String ATTR_POLICY_RESOURCES_CATEGORY = "policyResourceCategory";
    private static final String ATTR_POLICY_GROUPS             = "policyGroups";
    private static final String ATTR_POLICY_USERS              = "policyUsers";
    private static final String ATTR_POLICY_ROLES              = "policyRoles";
    private static final String ATTR_POLICY_VALIDITY           = "policyValiditySchedule";
    private static final String ATTR_POLICY_CONDITIONS         = "policyConditions";
    private static final String ATTR_POLICY_MASK_TYPE          = "policyMaskType";

    private static final String RESOURCE_SERVICE_DEF_PATH = "/service-defs/";
    private static final String RESOURCE_SERVICE_DEF_PATTERN = RESOURCE_SERVICE_DEF_PATH + "atlas-servicedef-%s.json";

    private EntityDiscoveryService discoveryService;
    private AtlasGraph                graph;
    private EntityGraphRetriever      entityRetriever;

    private PersonaCachePolicyTransformer personaTransformer;

    private AtlasEntityHeader service;

    @Inject
    public CachePolicyTransformerImpl(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
        this.graph                = new AtlasJanusGraph();
        this.entityRetriever      = new EntityGraphRetriever(graph, typeRegistry);

        personaTransformer = new PersonaCachePolicyTransformer(entityRetriever);

        try {
            this.discoveryService = new EntityDiscoveryService(typeRegistry, graph, null, null, null, null);
        } catch (AtlasException e) {
            LOG.error("Failed to initialize discoveryService");
            throw new AtlasBaseException(e.getCause());
        }
    }

    public AtlasEntityHeader getService() {
        return service;
    }

    public ServicePolicies getPolicies(String serviceName, String pluginId, Long lastUpdatedTime) {
        //TODO: return only if updated
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("CachePolicyTransformerImpl.getPolicies." + serviceName);

        ServicePolicies servicePolicies = new ServicePolicies();

        try {
            servicePolicies.setServiceName(serviceName);

            service = getServiceEntity(serviceName);
            servicePolicies.setPolicyVersion(-1L);
            servicePolicies.setPolicyUpdateTime(new Date());

            if (service != null) {
                List<RangerPolicy> policies = getServicePolicies(service);
                servicePolicies.setServiceName(serviceName);
                servicePolicies.setPolicies(policies);
                servicePolicies.setServiceId(service.getGuid());

                String serviceDefName = String.format(RESOURCE_SERVICE_DEF_PATTERN, serviceName);
                servicePolicies.setServiceDef(getResourceAsObject(serviceDefName, RangerServiceDef.class));


                //Process tag based policies
                String tagServiceName = (String) service.getAttribute("tagService");
                if (StringUtils.isNotEmpty(tagServiceName)) {
                    AtlasEntityHeader tagService = getServiceEntity(tagServiceName);

                    if (tagService != null) {
                        policies = getServicePolicies(tagService);
                        TagPolicies tagPolicies = new TagPolicies();

                        tagPolicies.setServiceName(tagServiceName);
                        tagPolicies.setPolicies(policies);
                        tagPolicies.setPolicyUpdateTime(new Date());
                        tagPolicies.setServiceId(tagService.getGuid());
                        tagPolicies.setPolicyVersion(-1L);

                        String tagServiceDefName =  String.format(RESOURCE_SERVICE_DEF_PATTERN, tagService.getAttribute(NAME));
                        tagPolicies.setServiceDef(getResourceAsObject(tagServiceDefName, RangerServiceDef.class));

                        servicePolicies.setTagPolicies(tagPolicies);
                    }
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Found {} policies", servicePolicies.getPolicies().size());
                }
            }

        } catch (Exception e) {
            LOG.error("ERROR in getPolicies {}: ", e.getMessage());
            return null;
        }

        RequestContext.get().endMetricRecord(recorder);
        return servicePolicies;
    }

    private List<RangerPolicy> getServicePolicies(AtlasEntityHeader service) throws AtlasBaseException, IOException {

        List<RangerPolicy> servicePolicies = new ArrayList<>();

        String serviceName = (String) service.getAttribute("name");
        String serviceType = (String) service.getAttribute("authServiceType");
        List<AtlasEntityHeader> atlasPolicies = getAtlasPolicies(serviceName);

        if (CollectionUtils.isNotEmpty(atlasPolicies)) {
            //transform policies
            servicePolicies = transformAtlasPoliciesToRangerPolicies(atlasPolicies, serviceType, serviceName);
        }
        return servicePolicies;
    }

    private List<RangerPolicy> transformAtlasPoliciesToRangerPolicies(List<AtlasEntityHeader> atlasPolicies,
                                                                      String serviceType,
                                                                      String serviceName) throws IOException, AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("CachePolicyTransformerImpl."+serviceName+".transformAtlasPoliciesToRangerPolicies");

        List<RangerPolicy> rangerPolicies = new ArrayList<>();
        try {
            for (AtlasEntityHeader atlasPolicy : atlasPolicies) {

                String policyCategory = getPolicyCategory(atlasPolicy);
                if (POLICY_CATEGORY_PERSONA.equals(policyCategory)) {

                    List<AtlasEntityHeader> transformedAtlasPolicies = personaTransformer.transform(atlasPolicy);
                    for (AtlasEntityHeader transformedPolicy : transformedAtlasPolicies) {
                        rangerPolicies.add(toRangerPolicy(transformedPolicy, serviceType));
                    }

                } else if (POLICY_CATEGORY_PURPOSE.equals(policyCategory)) {
                    rangerPolicies.add(toRangerPolicy(atlasPolicy, serviceType));

                } else {
                    rangerPolicies.add(toRangerPolicy(atlasPolicy, serviceType));
                }
            }

        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }

        return rangerPolicies;
    }

    private RangerPolicy toRangerPolicy(AtlasEntityHeader atlasPolicy, String serviceType) throws AtlasBaseException, IOException {
        RangerPolicy rangerPolicy = getRangerPolicy(atlasPolicy, serviceType);

        //GET policy Item
        setPolicyItems(rangerPolicy, atlasPolicy);

        //GET policy Resources
        setPolicyResources(rangerPolicy, atlasPolicy);

        return rangerPolicy;
    }

    private void setPolicyResources(RangerPolicy rangerPolicy, AtlasEntityHeader atlasPolicy) throws IOException {
        List<String> atlasResources = (List<String>) atlasPolicy.getAttribute("policyResources");

        Map<String, List<String>> resourceValuesMap = new HashMap<>();

        for (String atlasResource : atlasResources) {
            String resourceName = atlasResource.split(RESOURCES_SPLITTER)[0];

            if (!resourceValuesMap.containsKey(resourceName)) {
                List<String> applicables = atlasResources.stream().filter(x -> x.startsWith(resourceName + ":")).collect(Collectors.toList());
                List<String> values = applicables.stream().map(x -> x.split(RESOURCES_SPLITTER)[1]).collect(Collectors.toList());
                resourceValuesMap.put(resourceName, values);
            }
        }

        Map<String, RangerPolicyResource> resources = new HashMap<>();
        for (String key : resourceValuesMap.keySet()) {
            RangerPolicyResource resource = new RangerPolicyResource(resourceValuesMap.get(key), false, false);
            resources.put(key, resource);
        }

        rangerPolicy.setResources(resources);
    }

    private <T> T getResourceAsObject(String resourceName, Class<T> clazz) throws IOException {
        InputStream stream = getClass().getResourceAsStream(resourceName);
        return AtlasType.fromJson(stream, clazz);
    }

    private void setPolicyItems(RangerPolicy rangerPolicy, AtlasEntityHeader atlasPolicy) throws AtlasBaseException {

        String policyType = (String) atlasPolicy.getAttribute("policyType");

        List<String> users = (List<String>) atlasPolicy.getAttribute("policyUsers");
        List<String> groups = (List<String>) atlasPolicy.getAttribute("policyGroups");
        List<String> roles = (List<String>) atlasPolicy.getAttribute("policyRoles");

        List<RangerPolicyItemAccess> accesses = new ArrayList<>();
        List<String> actions = (List<String>) atlasPolicy.getAttribute("policyActions");

        actions.forEach(action -> accesses.add(new RangerPolicyItemAccess(action)));


        if ("allow".equals(policyType)) {
            RangerPolicyItem item = new RangerPolicyItem();
            item.setUsers(users);
            item.setGroups(groups);
            item.setRoles(roles);
            item.setAccesses(accesses);

            rangerPolicy.setPolicyItems(Collections.singletonList(item));

        } else if ("deny".equals(policyType)) {
            RangerPolicyItem item = new RangerPolicyItem();
            item.setUsers(users);
            item.setGroups(groups);
            item.setRoles(roles);
            item.setAccesses(accesses);

            rangerPolicy.setDenyPolicyItems(Collections.singletonList(item));

        } else if ("allowExceptions".equals(policyType)) {
            RangerPolicyItem item = new RangerPolicyItem();
            item.setUsers(users);
            item.setGroups(groups);
            item.setRoles(roles);
            item.setAccesses(accesses);

            rangerPolicy.setAllowExceptions(Collections.singletonList(item));

        } else if ("denyExceptions".equals(policyType)) {
            RangerPolicyItem item = new RangerPolicyItem();
            item.setUsers(users);
            item.setGroups(groups);
            item.setRoles(roles);
            item.setAccesses(accesses);

            rangerPolicy.setDenyExceptions(Collections.singletonList(item));

        } else if ("dataMask".equals(policyType)) {

            rangerPolicy.setPolicyType(RangerPolicy.POLICY_TYPE_DATAMASK);

            RangerDataMaskPolicyItem item = new RangerDataMaskPolicyItem();
            item.setUsers(users);
            item.setGroups(groups);
            item.setRoles(roles);
            item.setAccesses(accesses);

            String maskType = (String) atlasPolicy.getAttribute(ATTR_POLICY_MASK_TYPE);

            if (StringUtils.isEmpty(maskType)) {
                LOG.error("MASK type not found");
                throw new AtlasBaseException("MASK type not found");
            }

            RangerPolicyItemDataMaskInfo dataMaskInfo  = new RangerPolicyItemDataMaskInfo(maskType, null, null);
            item.setDataMaskInfo(dataMaskInfo);

            rangerPolicy.setDataMaskPolicyItems(Collections.singletonList(item));

        } else if ("rowFilter".equals(policyType)) {
            rangerPolicy.setPolicyType(RangerPolicy.POLICY_TYPE_ROWFILTER);
            //TODO
        }
    }

    private List<RangerPolicyItemCondition> getPolicyConditions(AtlasEntityHeader atlasPolicy) {
        List<RangerPolicyItemCondition> ret = new ArrayList<>();

        if (!atlasPolicy.hasAttribute("policyConditions")) {
            return null;
        }

        List<HashMap<String, Object>> conditions = (List<HashMap<String, Object>>) atlasPolicy.getAttribute("policyConditions");

        for (HashMap<String, Object> condition : conditions) {
            RangerPolicyItemCondition rangerCondition = new RangerPolicyItemCondition();

            rangerCondition.setType((String) condition.get("policyConditionType"));
            rangerCondition.setValues((List<String>) condition.get("policyConditionValues"));

            ret.add(rangerCondition);
        }
        return ret;
    }

    private List<RangerValiditySchedule> getPolicyValiditySchedule(AtlasEntityHeader atlasPolicy) {
        List<RangerValiditySchedule> ret = new ArrayList<>();

        if (!atlasPolicy.hasAttribute("policyValiditySchedule")) {
            return null;
        }

        List<HashMap<String, String>> validitySchedules = (List<HashMap<String, String>>) atlasPolicy.getAttribute("policyValiditySchedule");


        for (HashMap<String, String> validitySchedule : validitySchedules) {
            RangerValiditySchedule rangerValiditySchedule = new RangerValiditySchedule();

            rangerValiditySchedule.setStartTime(validitySchedule.get("policyValidityScheduleStartTime"));
            rangerValiditySchedule.setEndTime(validitySchedule.get("policyValidityScheduleEndTime"));
            rangerValiditySchedule.setTimeZone(validitySchedule.get("policyValidityScheduleTimezone"));

            ret.add(rangerValiditySchedule);
        }
        return ret;
    }

    private List<AtlasEntityHeader> getAtlasPolicies(String serviceName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("CachePolicyTransformerImpl."+service+".getAtlasPolicies");

        List<AtlasEntityHeader> ret = new ArrayList<>();
        try {
            IndexSearchParams indexSearchParams = new IndexSearchParams();
            Set<String> attributes = new HashSet<>();
            attributes.add(NAME);
            attributes.add(ATTR_POLICY_CATEGORY);
            attributes.add(ATTR_POLICY_SUB_CATEGORY);
            attributes.add(ATTR_POLICY_TYPE);
            attributes.add(ATTR_POLICY_SERVICE_NAME);
            attributes.add(ATTR_POLICY_USERS);
            attributes.add(ATTR_POLICY_GROUPS);
            attributes.add(ATTR_POLICY_ROLES);
            attributes.add(ATTR_POLICY_ACTIONS);
            attributes.add(ATTR_POLICY_RESOURCES);
            attributes.add(ATTR_POLICY_RESOURCES_CATEGORY);
            attributes.add(ATTR_POLICY_MASK_TYPE);
            attributes.add(ATTR_POLICY_PRIORITY);
            attributes.add(ATTR_POLICY_VALIDITY);
            attributes.add(ATTR_POLICY_CONDITIONS);
            attributes.add(ATTR_POLICY_IS_ENABLED);

            Map<String, Object> dsl = getMap("size", 0);

            List<Map<String, Object>> mustClauseList = new ArrayList<>();
            mustClauseList.add(getMap("term", getMap(ATTR_POLICY_SERVICE_NAME, serviceName)));
            mustClauseList.add(getMap("match", getMap("__state", Id.EntityState.ACTIVE)));

            dsl.put("query", getMap("bool", getMap("must", mustClauseList)));

            List<Map> sortList = new ArrayList<>(0);
            sortList.add(getMap("__timestamp", getMap("order", "asc")));
            sortList.add(getMap("__guid", getMap("order", "asc")));
            dsl.put("sort", sortList);

            indexSearchParams.setDsl(dsl);
            indexSearchParams.setAttributes(attributes);

            int from = 0;
            int size = 100;
            boolean found = true;

            do {
                dsl.put("from", from);
                dsl.put("size", size);
                indexSearchParams.setDsl(dsl);

                List<AtlasEntityHeader> headers = discoveryService.directIndexSearch(indexSearchParams).getEntities();
                if (headers != null) {
                    ret.addAll(headers);
                } else {
                    found = false;
                }

                from += size;

            } while (found && ret.size() % size == 0);

        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }

        return ret;
    }

    private AtlasEntityHeader getServiceEntity(String serviceName) throws AtlasBaseException {
        IndexSearchParams indexSearchParams = new IndexSearchParams();
        Set<String> attributes = new HashSet<>();
        attributes.add(NAME);
        attributes.add(ATTR_SERVICE_SERVICE_TYPE);
        attributes.add(ATTR_SERVICE_TAG_SERVICE);
        attributes.add(ATTR_SERVICE_IS_ENABLED);

        Map<String, Object> dsl = getMap("size", 1);

        List<Map<String, Object>> mustClauseList = new ArrayList<>();
        mustClauseList.add(getMap("term", getMap("__typeName.keyword", SERVICE_ENTITY_TYPE)));
        mustClauseList.add(getMap("term", getMap("name.keyword", serviceName)));
        mustClauseList.add(getMap("match", getMap("__state", Id.EntityState.ACTIVE)));

        dsl.put("query", getMap("bool", getMap("must", mustClauseList)));

        indexSearchParams.setDsl(dsl);
        indexSearchParams.setAttributes(attributes);

        AtlasSearchResult searchResult = discoveryService.directIndexSearch(indexSearchParams);

        if (searchResult.getEntities() != null) {
            return searchResult.getEntities().get(0);
        }

        return null;
    }

    private Map<String, Object> getMap(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    private RangerPolicy getRangerPolicy(AtlasEntityHeader atlasPolicy, String serviceType) {
        RangerPolicy policy = new RangerPolicy();

        //policy.setId(atlasPolicy.getGuid());
        policy.setName((String) atlasPolicy.getAttribute(QUALIFIED_NAME));
        policy.setService((String) atlasPolicy.getAttribute(ATTR_POLICY_SERVICE_NAME));
        policy.setServiceType(serviceType);
        policy.setGuid(atlasPolicy.getGuid());
        policy.setCreatedBy(atlasPolicy.getCreatedBy());
        policy.setCreateTime(atlasPolicy.getCreateTime());
        policy.setIsEnabled(getIsPolicyEnabled(atlasPolicy));

        policy.setConditions(getPolicyConditions(atlasPolicy));
        policy.setValiditySchedules(getPolicyValiditySchedule(atlasPolicy));

        if (atlasPolicy.hasAttribute(ATTR_POLICY_PRIORITY)) {
            policy.setPolicyPriority((Integer) atlasPolicy.getAttribute(ATTR_POLICY_PRIORITY));
        }

        return policy;
    }
}
