/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.authorize.simple;

import org.apache.atlas.authorize.AtlasActionTypes;
import org.apache.atlas.authorize.AtlasResourceTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class PolicyParser {

    private static Logger LOG = LoggerFactory.getLogger(PolicyParser.class);
    private static boolean isDebugEnabled = LOG.isDebugEnabled();
    public static final int POLICYNAME = 0;

    public static final int USER_INDEX = 1;
    public static final int USERNAME = 0;
    public static final int USER_AUTHORITIES = 1;

    public static final int GROUP_INDEX = 2;
    public static final int GROUPNAME = 0;
    public static final int GROUP_AUTHORITIES = 1;

    public static final int RESOURCE_INDEX = 3;
    public static final int RESOURCE_TYPE = 0;
    public static final int RESOURCE_NAME = 1;

    private List<AtlasActionTypes> getListOfAutorities(String auth) {
        if (isDebugEnabled) {
            LOG.debug("==> PolicyParser getListOfAutorities");
        }
        List<AtlasActionTypes> authorities = new ArrayList<>();

        for (int i = 0; i < auth.length(); i++) {
            char access = auth.toLowerCase().charAt(i);
            switch (access) {
                case 'r':
                    authorities.add(AtlasActionTypes.READ);
                    break;
                case 'w':
                    authorities.add(AtlasActionTypes.CREATE);
                    break;
                case 'u':
                    authorities.add(AtlasActionTypes.UPDATE);
                    break;
                case 'd':
                    authorities.add(AtlasActionTypes.DELETE);
                    break;

                default:
                    if (LOG.isErrorEnabled()) {
                        LOG.error("Invalid action: '{}'", access);
                    }
                    break;
            }
        }
        if (isDebugEnabled) {
            LOG.debug("<== PolicyParser getListOfAutorities");
        }
        return authorities;
    }

    public List<PolicyDef> parsePolicies(List<String> policies) {
        if (isDebugEnabled) {
            LOG.debug("==> PolicyParser parsePolicies");
        }
        List<PolicyDef> policyDefs = new ArrayList<>();
        for (String policy : policies) {
            PolicyDef policyDef = parsePolicy(policy);
            if (policyDef != null) {
                policyDefs.add(policyDef);
            }
        }
        if (isDebugEnabled) {
            LOG.debug("<== PolicyParser parsePolicies");
            LOG.debug(policyDefs.toString());
        }
        return policyDefs;
    }

    private PolicyDef parsePolicy(String data) {
        if (isDebugEnabled) {
            LOG.debug("==> PolicyParser parsePolicy");
        }
        PolicyDef def = null;
        String[] props = data.split(";;");

        if (props.length < RESOURCE_INDEX) {
            LOG.warn("skipping invalid policy line: {}", data);
        } else {
            def = new PolicyDef();
            def.setPolicyName(props[POLICYNAME]);
            parseUsers(props[USER_INDEX], def);
            parseGroups(props[GROUP_INDEX], def);
            parseResources(props[RESOURCE_INDEX], def);
            if (isDebugEnabled) {
                LOG.debug("policy successfully parsed!!!");
                LOG.debug("<== PolicyParser parsePolicy");
            }
        }
        return def;
    }

    private boolean validateEntity(String entity) {
        if (isDebugEnabled) {
            LOG.debug("==> PolicyParser validateEntity");
        }
        boolean isValidEntity = Pattern.matches("(.+:.+)+", entity);
        boolean isEmpty = entity.isEmpty();
        if (!isValidEntity || isEmpty) {
            if (isDebugEnabled) {
                LOG.debug("group/user/resource not properly define in Policy");
                LOG.debug("<== PolicyParser validateEntity");
            }
            return false;
        } else {
            if (isDebugEnabled) {
                LOG.debug("<== PolicyParser validateEntity");
            }
            return true;
        }

    }

    private void parseUsers(String usersDef, PolicyDef def) {
        if (isDebugEnabled) {
            LOG.debug("==> PolicyParser parseUsers");
        }
        String[] users = usersDef.split(",");
        String[] userAndRole = null;
        Map<String, List<AtlasActionTypes>> usersMap = new HashMap<>();
        if (validateEntity(usersDef)) {
            for (String user : users) {
                if (!Pattern.matches("(.+:.+)+", user)) {
                    continue;
                }
                userAndRole = user.split(":");
                if (def.getUsers() != null) {
                    usersMap = def.getUsers();
                }
                List<AtlasActionTypes> userAutorities = getListOfAutorities(userAndRole[USER_AUTHORITIES]);
                usersMap.put(userAndRole[USERNAME], userAutorities);
                def.setUsers(usersMap);
            }

        } else {
            def.setUsers(usersMap);
        }
        if (isDebugEnabled) {
            LOG.debug("<== PolicyParser parseUsers");
        }
    }

    private void parseGroups(String groupsDef, PolicyDef def) {
        if (isDebugEnabled) {
            LOG.debug("==> PolicyParser parseGroups");
        }
        String[] groups = groupsDef.split("\\,");
        String[] groupAndRole = null;
        Map<String, List<AtlasActionTypes>> groupsMap = new HashMap<>();
        if (validateEntity(groupsDef.trim())) {
            for (String group : groups) {
                if (!Pattern.matches("(.+:.+)+", group)) {
                    continue;
                }
                groupAndRole = group.split("[:]");
                if (def.getGroups() != null) {
                    groupsMap = def.getGroups();
                }
                List<AtlasActionTypes> groupAutorities = getListOfAutorities(groupAndRole[GROUP_AUTHORITIES]);
                groupsMap.put(groupAndRole[GROUPNAME], groupAutorities);
                def.setGroups(groupsMap);
            }

        } else {
            def.setGroups(groupsMap);
        }
        if (isDebugEnabled) {
            LOG.debug("<== PolicyParser parseGroups");
        }

    }

    private void parseResources(String resourceDef, PolicyDef def) {
        if (isDebugEnabled) {
            LOG.debug("==> PolicyParser parseResources");
        }
        String[] resources = resourceDef.split(",");
        String[] resourceTypeAndName = null;
        Map<AtlasResourceTypes, List<String>> resourcesMap = new HashMap<>();
        if (validateEntity(resourceDef)) {
            for (String resource : resources) {
                if (!Pattern.matches("(.+:.+)+", resource)) {
                    continue;
                }
                resourceTypeAndName = resource.split("[:]");
                if (def.getResources() != null) {
                    resourcesMap = def.getResources();
                }
                AtlasResourceTypes resourceType = null;
                String type = resourceTypeAndName[RESOURCE_TYPE].toUpperCase();
                if (type.equalsIgnoreCase("ENTITY")) {
                    resourceType = AtlasResourceTypes.ENTITY;
                } else if (type.equalsIgnoreCase("OPERATION")) {
                    resourceType = AtlasResourceTypes.OPERATION;
                } else if (type.equalsIgnoreCase("TYPE")) {
                    resourceType = AtlasResourceTypes.TYPE;
                } else if (type.equalsIgnoreCase("RELATIONSHIP")) {
                    resourceType = AtlasResourceTypes.RELATIONSHIP;
                } else {
                    LOG.warn(type + " is invalid resource please check PolicyStore file");
                    continue;
                }

                List<String> resourceList = resourcesMap.get(resourceType);
                if (resourceList == null) {
                    resourceList = new ArrayList<>();
                }
                resourceList.add(resourceTypeAndName[RESOURCE_NAME]);
                resourcesMap.put(resourceType, resourceList);
                def.setResources(resourcesMap);
            }
        } else {
            def.setResources(resourcesMap);
        }
        if (isDebugEnabled) {
            LOG.debug("<== PolicyParser parseResources");
        }
    }

}
