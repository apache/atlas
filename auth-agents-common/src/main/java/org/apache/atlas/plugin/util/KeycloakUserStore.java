/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.atlas.plugin.util;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.RequestContext;
import org.apache.atlas.auth.client.heracles.models.HeraclesGroupViewRepresentation;
import org.apache.atlas.auth.client.heracles.models.HeraclesRoleViewRepresentation;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.plugin.model.GroupInfo;
import org.apache.atlas.plugin.model.RangerRole;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.keycloak.representations.idm.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.atlas.auth.client.keycloak.AtlasKeycloakClient.getKeycloakClient;
import static org.apache.atlas.auth.client.heracles.AtlasHeraclesClient.getHeraclesClient;
import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.util.AccessControlUtils.ARGO_SERVICE_USER_NAME;
import static org.apache.atlas.repository.util.AccessControlUtils.BACKEND_SERVICE_USER_NAME;


public class KeycloakUserStore {
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("KeycloakUserStore");
    private static final Logger LOG = LoggerFactory.getLogger(KeycloakUserStore.class);

    private static int NUM_THREADS = 5;

    private static String LOGIN_EVENT_DETAIL_KEY = "custom_required_action";
    private static String LOGIN_EVENT_DETAIL_VALUE = "UPDATE_PROFILE";

    private static List<String> EVENT_TYPES = Arrays.asList("LOGIN");
    private static List<String> OPERATION_TYPES = Arrays.asList("CREATE", "UPDATE", "DELETE");
    private static List<String> RESOURCE_TYPES = Arrays.asList("USER", "GROUP", "REALM_ROLE", "CLIENT", "REALM_ROLE_MAPPING", "GROUP_MEMBERSHIP", "CLIENT_ROLE_MAPPING");

    private static String[] GROUPS_FETCH_COLUMNS = new String[]{"name"};

    private enum KEYCLOAK_FIELDS {
        ROLES,
        COMPOSITE_ROLES,
        GROUPS,
        USERS,

    }

    private final String serviceName;

    public KeycloakUserStore(String serviceName) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerRolesProvider(serviceName=" + serviceName + ").RangerRolesProvider()");
        }

        this.serviceName = serviceName;

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerRolesProvider(serviceName=" + serviceName + ").RangerRolesProvider()");
        }
    }

    public boolean isKeycloakSubjectsStoreUpdated(long cacheLastUpdatedTime) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("getKeycloakSubjectsStoreUpdatedTime");
        if (cacheLastUpdatedTime == -1) {
            return true;
        }

        long latestKeycloakEventTime = -1L;

        try {
            int size = 100;

            for (int from = 0; ; from += size) {

                List<AdminEventRepresentation> adminEvents = getKeycloakClient().getAdminEvents(OPERATION_TYPES,
                        null, null, null, null, null, null, null,
                        from, size);

                if (CollectionUtils.isEmpty(adminEvents) || cacheLastUpdatedTime > adminEvents.get(0).getTime()) {
                    break;
                }

                Optional<AdminEventRepresentation> event = adminEvents.stream().filter(x -> RESOURCE_TYPES.contains(x.getResourceType())).findFirst();

                if (event.isPresent()) {
                    latestKeycloakEventTime = event.get().getTime();
                    break;
                }
            }

            if (latestKeycloakEventTime > cacheLastUpdatedTime) {
                return true;
            }

            //check Events for user registration event via OKTA
            for (int from = 0; ; from += size) {

                List<EventRepresentation> events = getKeycloakClient().getEvents(EVENT_TYPES,
                        null, null, null, null, null, from, size);

                if (CollectionUtils.isEmpty(events) || cacheLastUpdatedTime > events.get(0).getTime()) {
                    break;
                }

                Optional<EventRepresentation> event = events.stream().filter(this::isUpdateProfileEvent).findFirst();

                if (event.isPresent()) {
                    latestKeycloakEventTime = event.get().getTime();
                    break;
                }
            }

            if (latestKeycloakEventTime > cacheLastUpdatedTime) {
                return true;
            }

        } catch (Exception e) {
            LOG.error("Error while fetching latest event time", e);
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }

        return false;
    }

    private boolean isUpdateProfileEvent(EventRepresentation event) {
        return MapUtils.isNotEmpty(event.getDetails()) &&
                event.getDetails().containsKey(LOGIN_EVENT_DETAIL_KEY) &&
                event.getDetails().get(LOGIN_EVENT_DETAIL_KEY).equals(LOGIN_EVENT_DETAIL_VALUE);
    }

    public RangerRoles loadRolesIfUpdated(long lastUpdatedTime) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("loadRolesIfUpdated");

        boolean isKeycloakUpdated = isKeycloakSubjectsStoreUpdated(lastUpdatedTime);
        if (!isKeycloakUpdated) {
            return null;
        }
        RangerRoles rangerRoles = new RangerRoles();
        Map<String, List<RangerRole.RoleMember>> roleUserMapping = new HashMap<>();
        Set<RangerRole> roleSet = new HashSet<>();

        int userSize = AtlasConfiguration.HERACLES_CLIENT_PAGINATION_SIZE.getInt();
        int userFrom = 0;
        List<UserRepresentation> userRetrievalResult;

        do {
            userRetrievalResult = getHeraclesClient().getUsersMappings(userFrom, userSize, new String[]{KEYCLOAK_FIELDS.ROLES.name().toLowerCase()});

            if (!CollectionUtils.isEmpty(userRetrievalResult)) {
                userRetrievalResult.forEach(user -> {
                    Set<String> userRoles = new HashSet<>(user.getRealmRoles());

                    userRoles.forEach(role -> roleUserMapping
                            .computeIfAbsent(role, k -> new ArrayList<>())
                            .add(new RangerRole.RoleMember(user.getUsername(), false))
                    );
                });

                userFrom += userSize;
            }

        } while (!CollectionUtils.isEmpty(userRetrievalResult) && userRetrievalResult.size() % userSize == 0);

        int roleSize = AtlasConfiguration.HERACLES_CLIENT_PAGINATION_SIZE.getInt();
        int roleFrom = 0;
        List<HeraclesRoleViewRepresentation> roleRetrievalResult;

        do {
            roleRetrievalResult = getHeraclesClient().getRolesMappings(roleFrom, roleSize, new String[]{KEYCLOAK_FIELDS.COMPOSITE_ROLES.name().toLowerCase(),
                    KEYCLOAK_FIELDS.GROUPS.name()});

            if (!CollectionUtils.isEmpty(roleRetrievalResult)) {
                roleRetrievalResult.forEach(role -> {
                    RangerRole rangerRole = new RangerRole();
                    rangerRole.setName(role.getName());
                    rangerRole.setGroups(role.getGroups().stream()
                            .map(x -> new RangerRole.RoleMember(x, false))
                            .collect(Collectors.toList()));
                    rangerRole.setUsers(roleUserMapping.get(role.getName()));
                    rangerRole.setRoles(role.getRoles().stream()
                            .map(x -> new RangerRole.RoleMember(x, false))
                            .collect(Collectors.toList()));

                    roleSet.add(rangerRole);
                });

                roleFrom += roleSize;
            }

        } while (!CollectionUtils.isEmpty(roleRetrievalResult) && roleRetrievalResult.size() % roleSize == 0);


        processDefaultRole(roleSet);
        LOG.info("Inverting roles");
        invertRoles(roleSet);

        rangerRoles.setRangerRoles(roleSet);
        rangerRoles.setServiceName(serviceName);

        Date current = new Date();
        rangerRoles.setRoleUpdateTime(current);
        rangerRoles.setServiceName(serviceName);
        rangerRoles.setRoleVersion(-1L);

        RequestContext.get().endMetricRecord(recorder);

        return rangerRoles;
    }

    private void extractUserGroupMapping(List<UserRepresentation> users, Map<String, Set<String>> userGroupMapping) {
        for (UserRepresentation user : users) {
            userGroupMapping.put(user.getUsername(), new HashSet<>(user.getGroups() == null ? Collections.emptyList() : user.getGroups()));
        }
    }

    public void invertRoles(Set<RangerRole> roleSet) {
        Map<String, RangerRole> roleMap = new HashMap<>();
        for (RangerRole role : roleSet) {
            RangerRole existingRole = roleMap.get(role.getName());
            if (existingRole != null) {
                existingRole.setGroups(role.getGroups());
                existingRole.setUsers(role.getUsers());
            } else {
                RangerRole newRole = new RangerRole();
                newRole.setName(role.getName());
                newRole.setUsers(role.getUsers());
                newRole.setGroups(role.getGroups());
                roleMap.put(role.getName(), newRole);
            }

            List<RangerRole.RoleMember> roles = role.getRoles();
            for (RangerRole.RoleMember roleMember : roles) {
                if (role.getName().equals("default-roles-default") && roleMember.getName().equals("$guest")) {
                    continue;
                }
                RangerRole existingRoleMember = roleMap.get(roleMember.getName());
                if (existingRoleMember != null) {
                    List<RangerRole.RoleMember> existingRoleMemberRoles = existingRoleMember.getRoles();
                    // If the role already present in existing role, then skip
                    if (existingRoleMemberRoles.stream().anyMatch(x -> x.getName().equals(role.getName()))) {
                        continue;
                    }
                    existingRoleMemberRoles.add(new RangerRole.RoleMember(role.getName(), false));
                } else {
                    RangerRole newRoleMember = new RangerRole();
                    newRoleMember.setName(roleMember.getName());
                    newRoleMember.setRoles(new ArrayList<>(Arrays.asList(new RangerRole.RoleMember(role.getName(), false))));
                    roleMap.put(roleMember.getName(), newRoleMember);
                }
            }
        }
        roleSet.clear();
        roleSet.addAll(roleMap.values());
    }

    private void processDefaultRole(Set<RangerRole> roleSet) {
        Optional<RangerRole> defaultRole = roleSet.stream().filter(x -> KEYCLOAK_ROLE_DEFAULT.equals(x.getName())).findFirst();

        if (defaultRole.isPresent()) {
            List<String> realmDefaultRoles = defaultRole.get().getRoles().stream().map(x -> x.getName()).collect(Collectors.toList());
            String tenantDefaultRealmUserRole = "";

            if (realmDefaultRoles.contains(KEYCLOAK_ROLE_ADMIN)) {
                tenantDefaultRealmUserRole = KEYCLOAK_ROLE_ADMIN;
            } else if (realmDefaultRoles.contains(KEYCLOAK_ROLE_MEMBER)) {
                tenantDefaultRealmUserRole = KEYCLOAK_ROLE_MEMBER;
            } else if (realmDefaultRoles.contains(KEYCLOAK_ROLE_GUEST)) {
                tenantDefaultRealmUserRole = KEYCLOAK_ROLE_GUEST;
            }

            String finalTenantDefaultRealmUserRole = tenantDefaultRealmUserRole;
            Optional<RangerRole> targetRole = roleSet.stream().filter(x -> finalTenantDefaultRealmUserRole.equals(x.getName())).findFirst();

            if (targetRole.isPresent()) {
                List<RangerRole.RoleMember> defaultUsers = new ArrayList<>(defaultRole.get().getUsers());
                List<RangerRole.RoleMember> nonGuestUsers = new ArrayList<>(0);

                Optional<RangerRole> adminRole = roleSet.stream().filter(x -> KEYCLOAK_ROLE_ADMIN.equals(x.getName())).findFirst();
                adminRole.ifPresent(rangerRole -> nonGuestUsers.addAll(rangerRole.getUsers()));

                Optional<RangerRole> memberRole = roleSet.stream().filter(x -> KEYCLOAK_ROLE_MEMBER.equals(x.getName())).findFirst();
                memberRole.ifPresent(rangerRole -> nonGuestUsers.addAll(rangerRole.getUsers()));

                Optional<RangerRole> apiTokenDefaultAccessRole = roleSet.stream().filter(x -> KEYCLOAK_ROLE_API_TOKEN.equals(x.getName())).findFirst();
                apiTokenDefaultAccessRole.ifPresent(rangerRole -> nonGuestUsers.addAll(rangerRole.getUsers()));

                defaultUsers.removeAll(nonGuestUsers);
                defaultUsers.remove(new RangerRole.RoleMember(ARGO_SERVICE_USER_NAME, false));
                defaultUsers.remove(new RangerRole.RoleMember(BACKEND_SERVICE_USER_NAME, false));

                targetRole.get().getUsers().addAll(defaultUsers);
            }
        }
    }

    public RangerUserStore loadUserStoreIfUpdated(long lastUpdatedTime) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("loadUserStoreIfUpdated");
        boolean isKeycloakUpdated = isKeycloakSubjectsStoreUpdated(lastUpdatedTime);
        if (!isKeycloakUpdated) {
            return null;
        }

        int userSize = 100;
        int userFrom = 0;
        boolean userFound = true;
        Map<String, Set<String>> userGroupMapping = new HashMap<>();
        List<UserRepresentation> ret = new ArrayList<>();

        do {
            List<UserRepresentation> users = getHeraclesClient().getUsersMappings(userFrom, userSize,
                    new String[]{KEYCLOAK_FIELDS.GROUPS.name().toLowerCase()});
            if (CollectionUtils.isEmpty(users)) {
                userFound = false;
            } else {
                ret.addAll(users);
                userFrom += userSize;
            }
            extractUserGroupMapping(users, userGroupMapping);

        } while (userFound && ret.size() % userSize == 0);

        // Fetch groups from Heracles to populate groupAttrMapping
        Set<GroupInfo> groups = loadGroupsFromHeracles();

        RangerUserStore userStore = new RangerUserStore(-1L, null, groups, userGroupMapping);
        userStore.setUserStoreUpdateTime(new Date());
        userStore.setServiceName(serviceName);

        RequestContext.get().endMetricRecord(recorder);


        return userStore;
    }

    /**
     * Fetches groups from Heracles API and converts them to GroupInfo objects.
     * This populates the groupAttrMapping in RangerUserStore for full group mapping support.
     * 
     * If the API call fails, returns an empty set and logs a warning (does not fail the entire loading).
     * 
     * @return Set of GroupInfo objects representing all groups from Heracles, or empty set on error
     */
    private Set<GroupInfo> loadGroupsFromHeracles() {
        LOG.info("loadGroupsFromHeracles: Starting to load groups from Heracles");
        
        Set<GroupInfo> groupInfoSet = new HashSet<>();
        AtlasPerfMetrics.MetricRecorder recorder = null;
        
        try {
            recorder = RequestContext.get().startMetricRecord("loadGroupsFromHeracles");
            
            int groupSize = AtlasConfiguration.HERACLES_CLIENT_PAGINATION_SIZE.getInt();
            int groupFrom = 0;
            List<HeraclesGroupViewRepresentation> groupRetrievalResult;
            
            LOG.info("loadGroupsFromHeracles: Using page size: {}", groupSize);
            
            do {
                LOG.info("loadGroupsFromHeracles: Fetching groups from Heracles: offset={}, limit={}", groupFrom, groupSize);
                groupRetrievalResult = getHeraclesClient().getGroupsMappingsV2(groupFrom, groupSize, GROUPS_FETCH_COLUMNS);
                
                if (!CollectionUtils.isEmpty(groupRetrievalResult)) {
                    LOG.info("loadGroupsFromHeracles: Received {} groups from Heracles in current page", groupRetrievalResult.size());
                    
                    for (HeraclesGroupViewRepresentation heraclesGroup : groupRetrievalResult) {
                        if (heraclesGroup.getName() != null) {
                            groupInfoSet.add(new GroupInfo(heraclesGroup.getName()));
                        } else {
                            LOG.warn("loadGroupsFromHeracles: Skipping group with null name from Heracles response");
                        }
                    }
                    
                    groupFrom += groupSize;
                } else {
                    LOG.info("loadGroupsFromHeracles: No groups received from Heracles (empty response)");
                }
                
            } while (!CollectionUtils.isEmpty(groupRetrievalResult) && groupRetrievalResult.size() == groupSize);
            
            LOG.info("loadGroupsFromHeracles: Successfully loaded {} groups from Heracles", groupInfoSet.size());
            
        } catch (Exception e) {
            LOG.error("loadGroupsFromHeracles: Error loading groups from Heracles. Group validation will not work properly. Error: {}", e.getMessage(), e);
            // Return empty set instead of failing - allows user store to still work for user validation
        } finally {
            if (recorder != null) {
                RequestContext.get().endMetricRecord(recorder);
            }
        }
        
        return groupInfoSet;
    }

    // Reserved attribute keys that should not be overwritten by custom attributes from Heracles
    // These keys are used for structured fields and cloud identity mapping
    private static final Set<String> RESERVED_ATTRIBUTE_KEYS = new HashSet<>(Arrays.asList(
            RangerUserStore.CLOUD_IDENTITY_NAME,  // "cloud_id" - used for cloud identity mapping
            "path",
            "realmId"
    ));
}
