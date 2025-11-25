package org.apache.atlas.authorizer.authorizers;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorizer.JsonToElasticsearchQuery;
import org.apache.atlas.authorizer.store.PoliciesStore;
import org.apache.atlas.plugin.model.RangerPolicy;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.authorizer.ABACAuthorizerUtils.DENY_POLICY_NAME_SUFFIX;
import static org.apache.atlas.authorizer.ABACAuthorizerUtils.MAX_CLAUSE_LIMIT;
import static org.apache.atlas.authorizer.ABACAuthorizerUtils.POLICY_TYPE_ALLOW;
import static org.apache.atlas.authorizer.ABACAuthorizerUtils.POLICY_TYPE_DENY;
import static org.apache.atlas.authorizer.authorizers.AuthorizerCommonUtil.getMap;

public class ListAuthorizer {
    private static final Logger LOG = LoggerFactory.getLogger(ListAuthorizer.class);
    private static final PoliciesStore policiesStore = PoliciesStore.getInstance();

    public static Map<String, Object> getElasticsearchDSL(String persona, String purpose, List<String> actions) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("ListAuthorizer.getElasticsearchDSL");
        Map<String, Object> allowDsl = getElasticsearchDSLForPolicyType(persona, purpose, actions, false, POLICY_TYPE_ALLOW);
        Map<String, Object> denyDsl = getElasticsearchDSLForPolicyType(persona, purpose, actions, false, POLICY_TYPE_DENY);
        Map<String, Object> finaDsl = new HashMap<>();
        if (allowDsl != null) {
            finaDsl.put("filter", allowDsl);
        }
        if (denyDsl != null) {
            finaDsl.put("must_not", denyDsl);
        }

        RequestContext.get().endMetricRecord(recorder);
        return getMap("bool", finaDsl);
    }

    public static Map<String, Object> getElasticsearchDSLForPolicyType(String persona, String purpose,
                                                                       List<String> actions, boolean requestMatchedPolicyId,
                                                                       String policyType) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("ListAuthorizer.getElasticsearchDSLForPolicyType."+ policyType);

        // TODO: consider resource and tag policies only if persona / purpose are empty, otherwise the alias will anyway take care of these policies
        List<RangerPolicy> resourcePolicies = policiesStore.getRelevantPolicies(persona, purpose, "atlas", actions, policyType);
        List<RangerPolicy> tagPolicies = policiesStore.getRelevantPolicies(persona, purpose, "atlas_tag", actions, policyType);
        List<RangerPolicy> abacPolicies = policiesStore.getRelevantPolicies(persona, purpose, "atlas_abac", actions, policyType);

        List<Map<String, Object>> shouldClauses = new ArrayList<>();
        if (requestMatchedPolicyId) {
            shouldClauses.addAll(getDSLForResourcePoliciesPerPolicy(resourcePolicies));
            shouldClauses.addAll(getDSLForTagPoliciesPerPolicy(tagPolicies));
            shouldClauses.addAll(getDSLForAbacPoliciesPerPolicy(abacPolicies));
        } else {
            shouldClauses.addAll(getDSLForResourcePolicies(resourcePolicies));
            Map<String, Object> tagDsl = getDSLForTagPolicies(tagPolicies);
            if (MapUtils.isNotEmpty(tagDsl)) {
                shouldClauses.add(tagDsl);
            }
            shouldClauses.addAll(getDSLForAbacPolicies(abacPolicies));
        }

        Map<String, Object> boolClause = new HashMap<>();
        if (shouldClauses.isEmpty()) {
            if (POLICY_TYPE_ALLOW.equals(policyType)) {
                boolClause.put("must_not", getMap("match_all", new HashMap<>()));
            } else {
                return null;
            }

        } else {
            if (shouldClauses.size() > MAX_CLAUSE_LIMIT) {
                List<Map<String, Object>> splittedShould = new ArrayList<>();
                List<List<Map<String, Object>>> partitionedShouldClause = Lists.partition(shouldClauses, MAX_CLAUSE_LIMIT);

                for (List<Map<String, Object>> chunk : partitionedShouldClause) {
                    splittedShould.add(getMap("bool", getMap("should", chunk)));
                }
                boolClause.put("should", splittedShould);

            } else {
                boolClause.put("should", shouldClauses);
            }

            boolClause.put("minimum_should_match", 1);
        }

        RequestContext.get().endMetricRecord(recorder);
        return getMap("bool", boolClause);
    }

    private static List<Map<String, Object>> getDSLForResourcePolicies(List<RangerPolicy> policies) {

        // To reduce the number of clauses
        List<String> combinedEntities = new ArrayList<>();
        Set<String> combinedEntityTypes = new HashSet<>();
        List<Map<String, Object>> shouldClauses = new ArrayList<>();

        for (RangerPolicy policy : policies) {
            if (MapUtils.isNotEmpty(policy.getResources())) {
                List<String> entities = new ArrayList<>(0);
                List<String> entityTypesRaw = new ArrayList<>(0);

                if (policy.getResources().get("entity") != null) {
                    entities = policy.getResources().get("entity").getValues();
                }

                if (policy.getResources().get("entity-type") != null) {
                    entityTypesRaw = policy.getResources().get("entity-type").getValues();
                }

                if (entities.contains("*") && entityTypesRaw.contains("*")) {
                    Map<String, String> emptyMap = new HashMap<>();
                    shouldClauses.clear();
                    shouldClauses.add(getMap("match_all",emptyMap));
                    break;
                }

                entities.remove("*");
                entityTypesRaw.remove("*");
                
                if (!entities.isEmpty() && entityTypesRaw.isEmpty()) {
                    combinedEntities.addAll(entities);
                } else if (entities.isEmpty() && !entityTypesRaw.isEmpty()) {
                    combinedEntityTypes.addAll(entityTypesRaw);
                } else if (!entities.isEmpty() && !entityTypesRaw.isEmpty()) {
                    Map<String, Object> dslForPolicyResources = getDSLForResources(entities, new HashSet<>(entityTypesRaw), null, null);
                    shouldClauses.add(dslForPolicyResources);
                }
            }
        }
        if (!combinedEntities.isEmpty()) {
            shouldClauses.add(getDSLForResources(combinedEntities, new HashSet<>(), null, null));
        }
        if (!combinedEntityTypes.isEmpty()) {
            shouldClauses.add(getDSLForResources(new ArrayList<>(), combinedEntityTypes, null, null));
        }
        LOG.info("ABAC_AUTH: FULL_RESTRICTION: filter for resource policies: {}", shouldClauses);
        return shouldClauses;
    }

    public static Map<String, Object> getDSLForResources(List<String> entities, Set<String> typeNames, List<String> classifications, String clauseName){
        List<Map<String, Object>> shouldClauses = new ArrayList<>();
        List<String> termsQualifiedNames = new ArrayList<>();
        for (String entity: entities) {
            if (!entity.equals("*") && !entity.isEmpty()) {
                String prefix = entity.substring(0, entity.length() - 1);
                if (entity.endsWith("*") && !(prefix.contains("*") || prefix.contains("?"))) {
                    shouldClauses.add(getMap("prefix", getMap("qualifiedName", prefix)));
                } else if (entity.contains("*") || entity.contains("?")) {
                    shouldClauses.add(getMap("wildcard", getMap("qualifiedName", entity)));
                } else {
                    termsQualifiedNames.add(entity);
                }
            }
        }
        if (!termsQualifiedNames.isEmpty()) {
            shouldClauses.add(getMap("terms", getMap("qualifiedName", termsQualifiedNames)));
        }

        Map<String, Object> boolClause = new HashMap<>();

        if (!shouldClauses.isEmpty()) {
            boolClause.put("should", shouldClauses);
            boolClause.put("minimum_should_match", 1);
        }

        List<Map<String, Object>> filterClauses = new ArrayList<>();

        if (!typeNames.isEmpty() && !typeNames.contains("*")) {
            List<Map<String, Object>> typeClauses = new ArrayList<>();
            typeClauses.add(getMap("terms", getMap("__typeName.keyword", typeNames)));
            typeClauses.add(getMap("terms", getMap("__superTypeNames.keyword", typeNames)));

            filterClauses.add(getMap("bool", getMap("should", typeClauses)));
        }

        if (classifications != null && !classifications.isEmpty() && !classifications.contains("*")) {
            List<Map<String, Object>> classificationClauses = new ArrayList<>();

            classificationClauses.add(getMap("terms", getMap("__traitNames", classifications)));
            classificationClauses.add(getMap("terms", getMap("__propagatedTraitNames", classifications)));

            filterClauses.add(getMap("bool", getMap("should", classificationClauses)));
        }

        if (!filterClauses.isEmpty()) {
            boolClause.put("filter", filterClauses);
        }

        if (clauseName != null) {
            boolClause.put("_name", clauseName);
        }

        return getMap("bool", boolClause);
    }

    public static Map<String, Object> getDSLForTagPolicies(List<RangerPolicy> policies) {
        // To reduce the number of clauses
        Set<String> allTags = new HashSet<>();

        for (RangerPolicy policy : policies) {
            if (MapUtils.isNotEmpty(policy.getResources())) {
                List<String> tags = new ArrayList<>(0);

                if (policy.getResources().get("tag") != null) {
                    tags = policy.getResources().get("tag").getValues();
                }

                if (!tags.isEmpty()) {
                    allTags.addAll(tags);
                }
            }
        }

        Map<String, Object> clauses = allTags.isEmpty() ? null : getDSLForTags(allTags);
        LOG.info("ABAC_AUTH: FULL_RESTRICTION: filter for tag policies: {}", clauses);

        return clauses;
    }

    public static List<Map<String, Object>> getDSLForAbacPolicies(List<RangerPolicy> policies) {
        List<String> dslList = new ArrayList<>();

        for (RangerPolicy policy : policies) {
            JsonNode entityFilterCriteriaNode = policy.getPolicyParsedFilterCriteria("entity");
            if (entityFilterCriteriaNode != null) {
                JsonNode dsl = JsonToElasticsearchQuery.convertJsonToQuery(entityFilterCriteriaNode);
                dslList.add(dsl.toString());
            }
        }

        List<Map<String, Object>> clauses = new ArrayList<>();
        for (String dsl: dslList) {
            String policyDSLBase64 = Base64.getEncoder().encodeToString(dsl.getBytes());;
            clauses.add(getMap("wrapper", getMap("query", policyDSLBase64)));
        }
        LOG.info("ABAC_AUTH: FULL_RESTRICTION: filter for abac policies: {}", dslList);
        return clauses;
    }

    public static List<Map<String, Object>> getDSLForResourcePoliciesPerPolicy(List<RangerPolicy> policies) {

        List<Map<String, Object>> shouldClauses = new ArrayList<>();

        for (RangerPolicy policy : policies) {
            if (MapUtils.isNotEmpty(policy.getResources())) {
                List<String> entities = new ArrayList<>(0);
                List<String> entityTypesRaw = new ArrayList<>(0);

                if (policy.getResources().get("entity") != null) {
                    entities = policy.getResources().get("entity").getValues();
                }

                if (policy.getResources().get("entity-type") != null) {
                    entityTypesRaw = policy.getResources().get("entity-type").getValues();
                }

                if (entities.contains("*") && entityTypesRaw.contains("*")) {
                    shouldClauses.clear();
                    shouldClauses.add(getMap("match_all", getMap("_name", policy.getGuid() + getPolicySuffix(policy))));
                    break;
                }

                Map<String, Object> dslForPolicyResources = getDSLForResources(entities, new HashSet<>(entityTypesRaw), null,
                        policy.getGuid() + getPolicySuffix(policy));
                shouldClauses.add(dslForPolicyResources);
            }
        }
        return shouldClauses;
    }

    public static String getPolicySuffix(RangerPolicy policy) {
        if (CollectionUtils.isNotEmpty(policy.getDenyPolicyItems())) {
            return DENY_POLICY_NAME_SUFFIX;
        }
        return "";
    }

    public static List<Map<String, Object>> getDSLForTagPoliciesPerPolicy(List<RangerPolicy> policies) {
        List<Map<String, Object>> shouldClauses = new ArrayList<>();


        for (RangerPolicy policy : policies) {
            if (MapUtils.isNotEmpty(policy.getResources())) {
                List<String> tags = new ArrayList<>(0);
                if (policy.getResources().get("tag") != null) {
                    tags = policy.getResources().get("tag").getValues();
                }

                if (!tags.isEmpty()) {

                    List<Map<String, Object>> tagsClauses = new ArrayList<>();
                    tagsClauses.add(getMap("terms", getMap("__traitNames", tags)));
                    tagsClauses.add(getMap("terms", getMap("__propagatedTraitNames", tags)));

                    Map<String, Object> shouldMap = getMap("should", tagsClauses);
                    shouldMap.put("minimum_should_match", 1);
                    shouldMap.put("_name", policy.getGuid() + getPolicySuffix(policy));

                    Map<String, Object> boolClause = getMap("bool", shouldMap);
                    shouldClauses.add(boolClause);
                }
            }
        }

        return shouldClauses;
    }

    public static List<Map<String, Object>> getDSLForAbacPoliciesPerPolicy(List<RangerPolicy> policies) {
        List<Map<String, Object>> clauses = new ArrayList<>();

        for (RangerPolicy policy : policies) {
            JsonNode entityFilterCriteriaNode = policy.getPolicyParsedFilterCriteria("entity");
            if (entityFilterCriteriaNode != null) {

                JsonNode dsl = JsonToElasticsearchQuery.convertJsonToQuery(entityFilterCriteriaNode);
                String policyDSLBase64 = Base64.getEncoder().encodeToString(dsl.toString().getBytes());

                Map<String, Object> shouldMap = getMap("should", getMap("wrapper", getMap("query", policyDSLBase64)));
                shouldMap.put("_name", policy.getGuid() + getPolicySuffix(policy));
                Map<String, Object> boolMap = getMap("bool", shouldMap);
                clauses.add(boolMap);
            }
        }


        return clauses;
    }

    private static Map<String, Object> getDSLForTags(Set<String> tags){
        List<Map<String, Object>> shouldClauses = new ArrayList<>();
        shouldClauses.add(getMap("terms", getMap("__traitNames", tags)));
        shouldClauses.add(getMap("terms", getMap("__propagatedTraitNames", tags)));

        Map<String, Object> boolClause = new HashMap<>();
        boolClause.put("should", shouldClauses);
        boolClause.put("minimum_should_match", 1);

        return getMap("bool", boolClause);
    }
}
