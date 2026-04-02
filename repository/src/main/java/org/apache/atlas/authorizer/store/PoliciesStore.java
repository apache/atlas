package org.apache.atlas.authorizer.store;

import org.apache.atlas.RequestContext;
import org.apache.atlas.authorizer.authorizers.AuthorizerCommonUtil;
import org.apache.atlas.plugin.model.RangerPolicy;
import org.apache.atlas.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.atlas.plugin.util.RangerRoles;
import org.apache.atlas.plugin.util.RangerUserStore;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.authorizer.ABACAuthorizerUtils.POLICY_TYPE_ALLOW;
import static org.apache.atlas.authorizer.ABACAuthorizerUtils.POLICY_TYPE_DENY;

public class PoliciesStore {

    private static final PoliciesStore INSTANCE = new PoliciesStore();

    private volatile PolicySnapshot resourceSnapshot = PolicySnapshot.EMPTY;
    private volatile PolicySnapshot tagSnapshot = PolicySnapshot.EMPTY;
    private volatile PolicySnapshot abacSnapshot = PolicySnapshot.EMPTY;

    private PoliciesStore() {} // private constructor

    public static PoliciesStore getInstance() {
        return INSTANCE;
    }

    public void setResourcePolicies(List<RangerPolicy> resourcePolicies) {
        List<RangerPolicy> normalized = normalizePolicies(resourcePolicies);
        this.resourceSnapshot = new PolicySnapshot(normalized, ServicePolicyIndex.build(normalized));
    }

    private List<RangerPolicy> getResourcePolicies() {
        return resourceSnapshot.policies;
    }

    public void setTagPolicies(List<RangerPolicy> tagPolicies) {
        List<RangerPolicy> normalized = normalizePolicies(tagPolicies);
        this.tagSnapshot = new PolicySnapshot(normalized, ServicePolicyIndex.build(normalized));
    }

    private List<RangerPolicy> getTagPolicies() {
        return tagSnapshot.policies;
    }

    public void setAbacPolicies(List<RangerPolicy> abacPolicies) {
        List<RangerPolicy> normalized = normalizePolicies(abacPolicies);
        this.abacSnapshot = new PolicySnapshot(normalized, ServicePolicyIndex.build(normalized));
    }

    public List<RangerPolicy> getAbacPolicies() {
        return abacSnapshot.policies;
    }

    public List<RangerPolicy> getRelevantPolicies(String persona, String purpose, String serviceName, List<String> actions, String policyType) {
        return getRelevantPolicies(persona, purpose, serviceName, actions, policyType, false);
    }

    public List<RangerPolicy> getRelevantPolicies(String persona, String purpose, String serviceName, List<String> actions, String policyType, boolean ignoreUser) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getRelevantPolicies");
        String policyQualifiedNamePrefix = null;
        if (persona != null && !persona.isEmpty()) {
            policyQualifiedNamePrefix = persona;
        } else if (purpose != null && !purpose.isEmpty()) {
            policyQualifiedNamePrefix = purpose;
        }

        ServicePolicyIndex policyIndex = getPolicyIndex(serviceName);
        List<RangerPolicy> actionFilteredPolicies;

        if (ignoreUser) {
            actionFilteredPolicies = policyIndex.getPoliciesForActions(actions, policyType);
        } else {
            String user = AuthorizerCommonUtil.getCurrentUserName();

            UsersStore usersStore = UsersStore.getInstance();
            RangerUserStore userStore = usersStore.getUserStore();
            List<String> groups = usersStore.getGroupsForUser(user, userStore);

            RangerRoles allRoles = usersStore.getAllRoles();
            List<String> roles = usersStore.getRolesForUser(user, groups, allRoles);
            roles.addAll(usersStore.getNestedRolesForUser(roles, allRoles));

            actionFilteredPolicies = policyIndex.getPoliciesForPrincipals(actions, policyType, user, groups, roles);
        }

        List<RangerPolicy> filteredPolicies = CollectionUtils.isNotEmpty(actionFilteredPolicies)
                ? getFilteredPoliciesForQualifiedName(actionFilteredPolicies, policyQualifiedNamePrefix)
                : new ArrayList<>(0);

        RequestContext.get().endMetricRecord(recorder);
        return filteredPolicies;
    }

    private List<RangerPolicy> getFilteredPoliciesForQualifiedName(List<RangerPolicy> policies, String qualifiedNamePrefix) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getFilteredPoliciesForQualifiedName");
        if (qualifiedNamePrefix != null && !qualifiedNamePrefix.isEmpty()) {
            List<RangerPolicy> filteredPolicies = new ArrayList<>();
            for(RangerPolicy policy : policies) {
                if (policy.getName().startsWith(qualifiedNamePrefix)) {
                    filteredPolicies.add(policy);
                }
            }
            return filteredPolicies;
        }

        RequestContext.get().endMetricRecord(recorder);
        return policies;
    }

    private static RangerPolicyItem getPolicyItemForType(RangerPolicy policy, String type) {
        RangerPolicyItem policyItem = null;

        if (StringUtils.isNotEmpty(type)) {
            if (POLICY_TYPE_ALLOW.equals(type) && !policy.getPolicyItems().isEmpty()) {
                policyItem = policy.getPolicyItems().get(0);
            } else if (POLICY_TYPE_DENY.equals(type) && !policy.getDenyPolicyItems().isEmpty()) {
                policyItem = policy.getDenyPolicyItems().get(0);
            }
        } else {
            if (!policy.getPolicyItems().isEmpty()) {
                policyItem = policy.getPolicyItems().get(0);
            } else if (!policy.getDenyPolicyItems().isEmpty()) {
                policyItem = policy.getDenyPolicyItems().get(0);
            }
        }

        return policyItem;
    }

    private static List<RangerPolicy> normalizePolicies(List<RangerPolicy> policies) {
        return CollectionUtils.isEmpty(policies) ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(policies));
    }

    private ServicePolicyIndex getPolicyIndex(String serviceName) {
        if ("atlas".equals(serviceName)) {
            return resourceSnapshot.index;
        } else if ("atlas_tag".equals(serviceName)) {
            return tagSnapshot.index;
        } else if ("atlas_abac".equals(serviceName)) {
            return abacSnapshot.index;
        }

        return ServicePolicyIndex.EMPTY;
    }

    /**
     * Immutable holder that groups a policy list with its pre-built index.
     * Swapped via a single volatile write so readers always see a consistent pair.
     */
    static final class PolicySnapshot {
        static final PolicySnapshot EMPTY = new PolicySnapshot(Collections.emptyList(), ServicePolicyIndex.EMPTY);

        final List<RangerPolicy> policies;
        final ServicePolicyIndex index;

        PolicySnapshot(List<RangerPolicy> policies, ServicePolicyIndex index) {
            this.policies = policies;
            this.index = index;
        }
    }

    private static final class ServicePolicyIndex {
        private static final ServicePolicyIndex EMPTY = new ServicePolicyIndex();

        private final Map<String, List<RangerPolicy>> policiesByAction;
        private final Map<String, List<RangerPolicy>> allowPoliciesByAction;
        private final Map<String, List<RangerPolicy>> denyPoliciesByAction;
        private final Map<String, PrincipalPolicyIndex> principalPoliciesByAction;
        private final Map<String, PrincipalPolicyIndex> allowPrincipalPoliciesByAction;
        private final Map<String, PrincipalPolicyIndex> denyPrincipalPoliciesByAction;

        private ServicePolicyIndex() {
            this.policiesByAction = Collections.emptyMap();
            this.allowPoliciesByAction = Collections.emptyMap();
            this.denyPoliciesByAction = Collections.emptyMap();
            this.principalPoliciesByAction = Collections.emptyMap();
            this.allowPrincipalPoliciesByAction = Collections.emptyMap();
            this.denyPrincipalPoliciesByAction = Collections.emptyMap();
        }

        private ServicePolicyIndex(Map<String, List<RangerPolicy>> policiesByAction,
                                   Map<String, List<RangerPolicy>> allowPoliciesByAction,
                                   Map<String, List<RangerPolicy>> denyPoliciesByAction,
                                   Map<String, PrincipalPolicyIndex> principalPoliciesByAction,
                                   Map<String, PrincipalPolicyIndex> allowPrincipalPoliciesByAction,
                                   Map<String, PrincipalPolicyIndex> denyPrincipalPoliciesByAction) {
            this.policiesByAction = policiesByAction;
            this.allowPoliciesByAction = allowPoliciesByAction;
            this.denyPoliciesByAction = denyPoliciesByAction;
            this.principalPoliciesByAction = principalPoliciesByAction;
            this.allowPrincipalPoliciesByAction = allowPrincipalPoliciesByAction;
            this.denyPrincipalPoliciesByAction = denyPrincipalPoliciesByAction;
        }

        private static ServicePolicyIndex build(List<RangerPolicy> policies) {
            if (CollectionUtils.isEmpty(policies)) {
                return EMPTY;
            }

            Map<String, List<RangerPolicy>> policiesByAction = new HashMap<>();
            Map<String, List<RangerPolicy>> allowPoliciesByAction = new HashMap<>();
            Map<String, List<RangerPolicy>> denyPoliciesByAction = new HashMap<>();
            Map<String, PrincipalPolicyIndex> principalPoliciesByAction = new HashMap<>();
            Map<String, PrincipalPolicyIndex> allowPrincipalPoliciesByAction = new HashMap<>();
            Map<String, PrincipalPolicyIndex> denyPrincipalPoliciesByAction = new HashMap<>();

            for (RangerPolicy policy : policies) {
                RangerPolicyItem defaultPolicyItem = getPolicyItemForType(policy, null);
                RangerPolicyItem allowPolicyItem = getPolicyItemForType(policy, POLICY_TYPE_ALLOW);
                RangerPolicyItem denyPolicyItem = getPolicyItemForType(policy, POLICY_TYPE_DENY);

                addPolicyByActions(policiesByAction, defaultPolicyItem, policy);
                addPolicyByActions(allowPoliciesByAction, allowPolicyItem, policy);
                addPolicyByActions(denyPoliciesByAction, denyPolicyItem, policy);

                addPolicyByPrincipalActions(principalPoliciesByAction, defaultPolicyItem, policy);
                addPolicyByPrincipalActions(allowPrincipalPoliciesByAction, allowPolicyItem, policy);
                addPolicyByPrincipalActions(denyPrincipalPoliciesByAction, denyPolicyItem, policy);
            }

            return new ServicePolicyIndex(
                    toUnmodifiableActionMap(policiesByAction),
                    toUnmodifiableActionMap(allowPoliciesByAction),
                    toUnmodifiableActionMap(denyPoliciesByAction),
                    toUnmodifiablePrincipalActionMap(principalPoliciesByAction),
                    toUnmodifiablePrincipalActionMap(allowPrincipalPoliciesByAction),
                    toUnmodifiablePrincipalActionMap(denyPrincipalPoliciesByAction));
        }

        private List<RangerPolicy> getPoliciesForActions(List<String> actions, String policyType) {
            if (CollectionUtils.isEmpty(actions)) {
                return new ArrayList<>(0);
            }

            Map<String, List<RangerPolicy>> actionMap = getActionMap(policyType);
            if (actionMap.isEmpty()) {
                return new ArrayList<>(0);
            }

            LinkedHashSet<RangerPolicy> dedupedPolicies = new LinkedHashSet<>();
            for (String action : actions) {
                List<RangerPolicy> actionPolicies = actionMap.get(action);
                if (CollectionUtils.isNotEmpty(actionPolicies)) {
                    dedupedPolicies.addAll(actionPolicies);
                }
            }

            return dedupedPolicies.isEmpty() ? new ArrayList<>(0) : new ArrayList<>(dedupedPolicies);
        }

        private List<RangerPolicy> getPoliciesForPrincipals(List<String> actions, String policyType, String user, List<String> groups, List<String> roles) {
            if (CollectionUtils.isEmpty(actions)) {
                return new ArrayList<>(0);
            }

            Map<String, PrincipalPolicyIndex> actionPrincipalMap = getPrincipalActionMap(policyType);
            if (actionPrincipalMap.isEmpty()) {
                return new ArrayList<>(0);
            }

            LinkedHashSet<RangerPolicy> dedupedPolicies = new LinkedHashSet<>();
            for (String action : actions) {
                PrincipalPolicyIndex principalPolicyIndex = actionPrincipalMap.get(action);
                if (principalPolicyIndex != null) {
                    principalPolicyIndex.collectPolicies(user, groups, roles, dedupedPolicies);
                }
            }

            return dedupedPolicies.isEmpty() ? new ArrayList<>(0) : new ArrayList<>(dedupedPolicies);
        }

        private Map<String, List<RangerPolicy>> getActionMap(String policyType) {
            return StringUtils.isEmpty(policyType)
                    ? policiesByAction
                    : POLICY_TYPE_ALLOW.equals(policyType) ? allowPoliciesByAction : denyPoliciesByAction;
        }

        private Map<String, PrincipalPolicyIndex> getPrincipalActionMap(String policyType) {
            return StringUtils.isEmpty(policyType)
                    ? principalPoliciesByAction
                    : POLICY_TYPE_ALLOW.equals(policyType) ? allowPrincipalPoliciesByAction : denyPrincipalPoliciesByAction;
        }

        private static void addPolicyByActions(Map<String, List<RangerPolicy>> index, RangerPolicyItem policyItem, RangerPolicy policy) {
            if (policyItem == null || CollectionUtils.isEmpty(policyItem.getAccesses())) {
                return;
            }

            for (RangerPolicy.RangerPolicyItemAccess access : policyItem.getAccesses()) {
                if (access != null && StringUtils.isNotEmpty(access.getType())) {
                    index.computeIfAbsent(access.getType(), k -> new ArrayList<>()).add(policy);
                }
            }
        }

        private static void addPolicyByPrincipalActions(Map<String, PrincipalPolicyIndex> index, RangerPolicyItem policyItem, RangerPolicy policy) {
            if (policyItem == null || CollectionUtils.isEmpty(policyItem.getAccesses())) {
                return;
            }

            for (RangerPolicy.RangerPolicyItemAccess access : policyItem.getAccesses()) {
                if (access != null && StringUtils.isNotEmpty(access.getType())) {
                    PrincipalPolicyIndex principalPolicyIndex = index.computeIfAbsent(access.getType(), ignored -> new PrincipalPolicyIndex());
                    principalPolicyIndex.addPolicy(policyItem, policy);
                }
            }
        }

        private static Map<String, List<RangerPolicy>> toUnmodifiableActionMap(Map<String, List<RangerPolicy>> index) {
            if (index.isEmpty()) {
                return Collections.emptyMap();
            }

            Map<String, List<RangerPolicy>> ret = new HashMap<>(index.size());
            for (Map.Entry<String, List<RangerPolicy>> entry : index.entrySet()) {
                ret.put(entry.getKey(), Collections.unmodifiableList(new ArrayList<>(entry.getValue())));
            }
            return Collections.unmodifiableMap(ret);
        }

        private static Map<String, PrincipalPolicyIndex> toUnmodifiablePrincipalActionMap(Map<String, PrincipalPolicyIndex> index) {
            if (index.isEmpty()) {
                return Collections.emptyMap();
            }

            Map<String, PrincipalPolicyIndex> ret = new HashMap<>(index.size());
            for (Map.Entry<String, PrincipalPolicyIndex> entry : index.entrySet()) {
                ret.put(entry.getKey(), entry.getValue().freeze());
            }

            return Collections.unmodifiableMap(ret);
        }
    }

    private static final class PrincipalPolicyIndex {
        private static final String PUBLIC_GROUP = "public";

        private final Map<String, List<RangerPolicy>> policiesByUser = new HashMap<>();
        private final Map<String, List<RangerPolicy>> policiesByGroup = new HashMap<>();
        private final Map<String, List<RangerPolicy>> policiesByRole = new HashMap<>();
        private final List<RangerPolicy> publicPolicies = new ArrayList<>();

        private void addPolicy(RangerPolicyItem policyItem, RangerPolicy policy) {
            addPolicyForPrincipals(policiesByUser, policyItem.getUsers(), policy);
            addPolicyForPrincipals(policiesByRole, policyItem.getRoles(), policy);

            List<String> policyGroups = policyItem.getGroups();
            if (CollectionUtils.isNotEmpty(policyGroups)) {
                for (String group : policyGroups) {
                    if (PUBLIC_GROUP.equals(group)) {
                        publicPolicies.add(policy);
                    } else if (group != null) {
                        policiesByGroup.computeIfAbsent(group, ignored -> new ArrayList<>()).add(policy);
                    }
                }
            }
        }

        private void collectPolicies(String user, List<String> groups, List<String> roles, LinkedHashSet<RangerPolicy> output) {
            if (!publicPolicies.isEmpty()) {
                output.addAll(publicPolicies);
            }

            if (user != null) {
                List<RangerPolicy> userPolicies = policiesByUser.get(user);
                if (CollectionUtils.isNotEmpty(userPolicies)) {
                    output.addAll(userPolicies);
                }
            }

            if (CollectionUtils.isNotEmpty(groups)) {
                for (String group : groups) {
                    List<RangerPolicy> groupPolicies = policiesByGroup.get(group);
                    if (CollectionUtils.isNotEmpty(groupPolicies)) {
                        output.addAll(groupPolicies);
                    }
                }
            }

            if (CollectionUtils.isNotEmpty(roles)) {
                for (String role : roles) {
                    List<RangerPolicy> rolePolicies = policiesByRole.get(role);
                    if (CollectionUtils.isNotEmpty(rolePolicies)) {
                        output.addAll(rolePolicies);
                    }
                }
            }
        }

        private PrincipalPolicyIndex freeze() {
            return new PrincipalPolicyIndex(
                    toUnmodifiableListMap(policiesByUser),
                    toUnmodifiableListMap(policiesByGroup),
                    toUnmodifiableListMap(policiesByRole),
                    Collections.unmodifiableList(new ArrayList<>(publicPolicies)));
        }

        private PrincipalPolicyIndex(Map<String, List<RangerPolicy>> policiesByUser,
                                     Map<String, List<RangerPolicy>> policiesByGroup,
                                     Map<String, List<RangerPolicy>> policiesByRole,
                                     List<RangerPolicy> publicPolicies) {
            this.policiesByUser.putAll(policiesByUser);
            this.policiesByGroup.putAll(policiesByGroup);
            this.policiesByRole.putAll(policiesByRole);
            this.publicPolicies.addAll(publicPolicies);
        }

        private PrincipalPolicyIndex() {
        }

        private static void addPolicyForPrincipals(Map<String, List<RangerPolicy>> index, List<String> principals, RangerPolicy policy) {
            if (CollectionUtils.isEmpty(principals)) {
                return;
            }

            for (String principal : principals) {
                if (principal != null) {
                    index.computeIfAbsent(principal, ignored -> new ArrayList<>()).add(policy);
                }
            }
        }

        private static Map<String, List<RangerPolicy>> toUnmodifiableListMap(Map<String, List<RangerPolicy>> source) {
            if (source.isEmpty()) {
                return Collections.emptyMap();
            }

            Map<String, List<RangerPolicy>> ret = new HashMap<>(source.size());
            for (Map.Entry<String, List<RangerPolicy>> entry : source.entrySet()) {
                ret.put(entry.getKey(), Collections.unmodifiableList(new ArrayList<>(entry.getValue())));
            }

            return Collections.unmodifiableMap(ret);
        }
    }
}
