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
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.keycloak.client.KeycloakClient;
import org.apache.atlas.plugin.model.RangerRole;
import org.apache.atlas.plugin.service.RangerBasePlugin;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.collections.CollectionUtils;
import org.keycloak.admin.client.resource.RoleResource;
import org.keycloak.representations.idm.AdminEventRepresentation;
import org.keycloak.representations.idm.GroupRepresentation;
import org.keycloak.representations.idm.RoleRepresentation;
import org.keycloak.representations.idm.UserRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.Constants.KEYCLOAK_ROLE_ADMIN;
import static org.apache.atlas.repository.Constants.KEYCLOAK_ROLE_DEFAULT;
import static org.apache.atlas.repository.Constants.KEYCLOAK_ROLE_GUEST;
import static org.apache.atlas.repository.Constants.KEYCLOAK_ROLE_MEMBER;


public class KeycloakUserStore {
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("KeycloakUserStore");
    private static final Logger LOG = LoggerFactory.getLogger(KeycloakUserStore.class);

    private static int NUM_THREADS = 5;

    List<String> OPERATION_TYPES = Arrays.asList("CREATE", "UPDATE", "DELETE");
    List<String> RESOURCE_TYPES = Arrays.asList("USER", "GROUP", "REALM_ROLE", "CLIENT", "REALM_ROLE_MAPPING", "GROUP_MEMBERSHIP", "CLIENT_ROLE_MAPPING");

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

    public long getKeycloakSubjectsStoreUpdatedTime() throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("getKeycloakSubjectsStoreUpdatedTime");
        KeycloakClient keycloakClient = KeycloakClient.getKeycloakClient();
        long latestEventTime = -1L;

        try {
            int from = 0;
            int size = 100;


            while (latestEventTime == -1L) {
                List<AdminEventRepresentation> adminEvents = keycloakClient.getRealm().getAdminEvents(OPERATION_TYPES,
                        null, null, null, null, null, null, null,
                        from, size);
                Optional<AdminEventRepresentation> event = adminEvents.stream().filter(x -> RESOURCE_TYPES.contains(x.getResourceType())).findFirst();
                if (event.isPresent()) {
                    latestEventTime = event.get().getTime();
                }
                from += size;
            }
        } catch (Exception e) {
            LOG.error("Error while fetching latest event time", e);
        } finally {
            keycloakClient.getRealm().getClientSessionStats();
            RequestContext.get().endMetricRecord(metricRecorder);
        }
        return latestEventTime;
    }

    public RangerRoles loadRolesIfUpdated(long lastUpdatedTime) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("loadRolesIfUpdated");

        long keycloakUpdateTime = getKeycloakSubjectsStoreUpdatedTime();
        if (keycloakUpdateTime <= lastUpdatedTime) {
            LOG.info("loadRolesIfUpdated: Skipping as no update found");
            return null;
        }

        List<RoleRepresentation> kRoles = KeycloakClient.getKeycloakClient().getAllRoles();
        LOG.info("Found {} keycloak roles", kRoles.size());

        Set<RangerRole> roleSet = new HashSet<>();
        RangerRoles rangerRoles = new RangerRoles();
        List<UserRepresentation> userNamesList = new ArrayList<>();

        submitCallablesAndWaitToFinish("RoleSubjectsFetcher",
                kRoles.stream()
                        .map(x -> new RoleSubjectsFetcher(x, roleSet, userNamesList))
                        .collect(Collectors.toList()));

        processDefaultRole(roleSet);

        rangerRoles.setRangerRoles(roleSet);
        rangerRoles.setServiceName(serviceName);

        Date current = new Date();
        rangerRoles.setRoleUpdateTime(current);
        rangerRoles.setServiceName(serviceName);
        rangerRoles.setRoleVersion(-1L);

        RequestContext.get().endMetricRecord(recorder);

        return rangerRoles;
    }

    private void processDefaultRole(Set<RangerRole> roleSet) {
        Optional<RangerRole> defaultRole = roleSet.stream().filter(x -> KEYCLOAK_ROLE_DEFAULT.equals(x.getName())).findFirst();

        if (defaultRole.isPresent()){
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

                defaultUsers.removeAll(nonGuestUsers);

                targetRole.get().getUsers().addAll(defaultUsers);
            }
        }
    }

    public RangerUserStore loadUserStoreIfUpdated(long lastUpdatedTime) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("loadUserStoreIfUpdated");

        long keycloakUpdateTime = getKeycloakSubjectsStoreUpdatedTime();
        if (keycloakUpdateTime <= lastUpdatedTime) {
            LOG.info("loadUserStoreIfUpdated: Skipping as no update found");
            return null;
        }

        Map<String, Set<String>> userGroupMapping = new HashMap<>();

        List<UserRepresentation> kUsers = KeycloakClient.getKeycloakClient().getAllUsers();
        LOG.info("Found {} keycloak users", kUsers.size());

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
                RoleResource roleResource = KeycloakClient.getKeycloakClient().getRealm().roles().get(kRole.getName());

                //get all groups for Roles
                Thread groupsFetcher = new Thread(() -> {
                    int start = 0;
                    int size = 500;
                    boolean found = true;
                    Set<GroupRepresentation> ret = new HashSet<>();

                    do {
                        Set<GroupRepresentation> kGroups = roleResource.getRoleGroupMembers(start, size);
                        if (CollectionUtils.isNotEmpty(kGroups)) {
                            ret.addAll(kGroups);
                            start += size;
                        } else {
                            found = false;
                        }

                    } while (found && ret.size() % size == 0);

                    rangerRole.setGroups(keycloakGroupsToRangerRoleMember(ret));
                });
                groupsFetcher.start();

                //get all users for Roles
                Thread usersFetcher = new Thread(() -> {
                    int start = 0;
                    int size = 500;
                    boolean found = true;
                    Set<UserRepresentation> ret = new HashSet<>();

                    do {
                        Set<UserRepresentation> userRepresentations = roleResource.getRoleUserMembers(start, size);
                        if (CollectionUtils.isNotEmpty(userRepresentations)) {
                            ret.addAll(userRepresentations);
                            start += size;
                        } else {
                            found = false;
                        }

                    } while (found && ret.size() % size == 0);

                    rangerRole.setUsers(keycloakUsersToRangerRoleMember(ret));
                    userNamesList.addAll(ret);
                });
                usersFetcher.start();

                //get all roles for Roles
                Thread subRolesFetcher = new Thread(() -> {
                    Set<RoleRepresentation> kSubRoles = roleResource.getRoleComposites();
                    rangerRole.setRoles(keycloakRolesToRangerRoleMember(kSubRoles));
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
                List<GroupRepresentation> kGroups = KeycloakClient.getKeycloakClient().getRealm().users().get(kUser.getId()).groups();
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
