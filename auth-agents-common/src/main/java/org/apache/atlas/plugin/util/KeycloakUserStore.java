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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.plugin.model.RangerRole;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.keycloak.representations.idm.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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

    public static ExecutorService getExecutorService(String namePattern) {
        ExecutorService service = Executors.newFixedThreadPool(NUM_THREADS,
                new ThreadFactoryBuilder().setNameFormat(namePattern + Thread.currentThread().getName())
                        .build());
        return service;
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

        List<RoleRepresentation> kRoles = getKeycloakClient().getAllRoles();
        LOG.info("Found {} keycloak roles", kRoles.size());

        Set<RangerRole> roleSet = new HashSet<>();
        RangerRoles rangerRoles = new RangerRoles();
        List<UserRepresentation> userNamesList = new ArrayList<>();

        submitCallablesAndWaitToFinish("RoleSubjectsFetcher",
                kRoles.stream()
                        .map(x -> new RoleSubjectsFetcher(x, roleSet, userNamesList))
                        .collect(Collectors.toList()));

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

        Map<String, Set<String>> userGroupMapping = new HashMap<>();
        List<UserRepresentation> kUsers = getHeraclesClient().getAllUsers();
        LOG.info("Found {} users", kUsers.size());
        List<Callable<Object>> callables = new ArrayList<>();
        kUsers.forEach(x -> callables.add(new UserGroupsFetcher(x, userGroupMapping)));

        submitCallablesAndWaitToFinish("UserGroupsFetcher", callables);

        RangerUserStore userStore = new RangerUserStore();
        userStore.setUserGroupMapping(userGroupMapping);
        Date current = new Date();
        userStore.setUserStoreUpdateTime(current);
        userStore.setServiceName(serviceName);
        userStore.setUserStoreVersion(-1L);

        RequestContext.get().endMetricRecord(recorder);

        return userStore;
    }

    private static RangerRole keycloakRoleToRangerRole(RoleRepresentation kRole) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("keycloakRolesToRangerRoles");

        RangerRole rangerRole = new RangerRole();
        rangerRole.setName(kRole.getName());
        rangerRole.setDescription(kRole.getDescription() + " " + kRole.getId());

        RequestContext.get().endMetricRecord(recorder);
        return rangerRole;
    }

    private static List<RangerRole.RoleMember> keycloakGroupsToRangerRoleMember(Set<GroupRepresentation> kGroups) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("keycloakGroupsToRangerRoleMember");
        List<RangerRole.RoleMember> rangerGroups = new ArrayList<>();

        for (GroupRepresentation kGroup : kGroups) {
            //TODO: Revisit isAdmin flag
            rangerGroups.add(new RangerRole.RoleMember(kGroup.getName(), false));
        }

        RequestContext.get().endMetricRecord(recorder);
        return rangerGroups;
    }

    private static List<RangerRole.RoleMember> keycloakUsersToRangerRoleMember(Set<UserRepresentation> kUsers) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("keycloakUsersToRangerRoleMember");
        List<RangerRole.RoleMember> rangerUsers = new ArrayList<>();

        for (UserRepresentation kUser : kUsers) {
            //TODO: Revisit isAdmin flag
            rangerUsers.add(new RangerRole.RoleMember(kUser.getUsername(), false));
        }

        RequestContext.get().endMetricRecord(recorder);
        return rangerUsers;
    }

    private static List<RangerRole.RoleMember> keycloakRolesToRangerRoleMember(Set<RoleRepresentation> kRoles) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("keycloakRolesToRangerRoleMember");
        List<RangerRole.RoleMember> rangerRoles = new ArrayList<>();

        for (RoleRepresentation kRole : kRoles) {
            //TODO: Revisit isAdmin flag
            rangerRoles.add(new RangerRole.RoleMember(kRole.getName(), false));
        }

        RequestContext.get().endMetricRecord(recorder);
        return rangerRoles;
    }

    protected static <T> void submitCallablesAndWaitToFinish(String threadName, List<Callable<T>> callables) throws AtlasBaseException {
        ExecutorService service = getExecutorService(threadName + "-%d-");
        try {

            LOG.info("Submitting callables: {}", threadName);
            callables.forEach(service::submit);

            service.shutdown();

            boolean terminated = service.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            LOG.info("awaitTermination done: {}", threadName);

            if (!terminated) {
                LOG.warn("Time out occurred while waiting to complete {}", threadName);
            }
        } catch (InterruptedException e) {
            throw new AtlasBaseException();
        }
    }

    static class RoleSubjectsFetcher implements Callable<RangerRole> {
        private Set<RangerRole> roleSet;
        private RoleRepresentation kRole;
        List<UserRepresentation> userNamesList;

        public RoleSubjectsFetcher(RoleRepresentation kRole,
                                   Set<RangerRole> roleSet,
                                   List<UserRepresentation> userNamesList) {
            this.kRole = kRole;
            this.roleSet = roleSet;
            this.userNamesList = userNamesList;
        }

        @Override
        public RangerRole call() throws Exception {
            AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("roleSubjectsFetcher");
            final RangerRole rangerRole = keycloakRoleToRangerRole(kRole);

            try {
                //get all groups for Roles
                Thread groupsFetcher = new Thread(() -> {
                    int start = 0;
                    int size = AtlasConfiguration.KEYCLOAK_ADMIN_API_RESOURCE_PAGINATION_SIZE.getInt();
                    boolean found = true;
                    Set<GroupRepresentation> ret = new HashSet<>();

                    do {
                        try {
                            Set<GroupRepresentation> kGroups = getKeycloakClient().getRoleGroupMembers(kRole.getName(), start, size);
                            if (CollectionUtils.isNotEmpty(kGroups)) {
                                ret.addAll(kGroups);
                                start += size;
                            } else {
                                found = false;
                            }
                        } catch (Exception e) {
                            LOG.error("Failed to get group members with role", e);
                            throw new RuntimeException(e);
                        }

                    } while (found && ret.size() % size == 0);

                    rangerRole.setGroups(keycloakGroupsToRangerRoleMember(ret));
                });
                groupsFetcher.start();

                //get all users for Roles
                Thread usersFetcher = new Thread(() -> {
                    int start = 0;
                    int size = 100;
                    boolean found = true;
                    Set<UserRepresentation> ret = new HashSet<>();

                    do {
                        try {
                            Set<UserRepresentation> userRepresentations = getHeraclesClient().getRoleUserMembers(kRole.getName(), start, size);
                            if (CollectionUtils.isNotEmpty(userRepresentations)) {
                                ret.addAll(userRepresentations);
                                start += size;
                            } else {
                                found = false;
                            }
                        } catch (Exception e) {
                            LOG.error("Failed to get users for role {}", kRole.getName(), e);
                            throw new RuntimeException(e);
                        }

                    } while (found && ret.size() % size == 0);

                    rangerRole.setUsers(keycloakUsersToRangerRoleMember(ret));
                    userNamesList.addAll(ret);
                });
                usersFetcher.start();

                //get all roles for Roles
                Thread subRolesFetcher = new Thread(() -> {
                    Set<RoleRepresentation> kSubRoles = null;
                    try {
                        kSubRoles = getKeycloakClient().getRoleComposites(kRole.getName());
                        rangerRole.setRoles(keycloakRolesToRangerRoleMember(kSubRoles));
                    } catch (AtlasBaseException e) {
                        LOG.error("Failed to get composite for role {}", kRole.getName(), e);
                        throw new RuntimeException(e);
                    }
                });
                subRolesFetcher.start();

                try {
                    groupsFetcher.join();
                    usersFetcher.join();
                    subRolesFetcher.join();
                } catch (InterruptedException e) {
                    LOG.error("Failed to wait for threads to complete: {}", kRole.getName());
                    e.printStackTrace();
                }

                RequestContext.get().endMetricRecord(recorder);
                roleSet.add(rangerRole);
            } catch (Exception e) {
                LOG.error("RoleSubjectsFetcher: Failed to process role {}: {}", kRole.getName(), e.getMessage());
            } finally {
                RequestContext.get().endMetricRecord(recorder);
            }

            return rangerRole;
        }
    }

    static class UserGroupsFetcher implements Callable {
        private Map<String, Set<String>> userGroupMapping;
        private UserRepresentation kUser;

        public UserGroupsFetcher(UserRepresentation kUser, Map<String, Set<String>> userGroupMapping) {
            this.kUser = kUser;
            this.userGroupMapping = userGroupMapping;
        }

        @Override
        public Object call() throws Exception {
            AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("userGroupsFetcher");

            try {
                List<GroupRepresentation> kGroups = getKeycloakClient().getGroupsForUserById(kUser.getId());
                userGroupMapping.put(kUser.getUsername(),
                        kGroups.stream()
                                .map(GroupRepresentation::getName)
                                .collect(Collectors.toSet()));

            } catch (Exception e) {
                LOG.error("UserGroupsFetcher: Failed to process user {}: {}", kUser.getUsername(), e.getMessage());
            } finally {
                RequestContext.get().endMetricRecord(recorder);
            }

            return null;
        }
    }
}
