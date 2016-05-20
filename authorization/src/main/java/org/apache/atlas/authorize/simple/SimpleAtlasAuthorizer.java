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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Map;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.authorize.AtlasAccessRequest;
import org.apache.atlas.authorize.AtlasActionTypes;
import org.apache.atlas.authorize.AtlasAuthorizationException;
import org.apache.atlas.authorize.AtlasAuthorizer;
import org.apache.atlas.authorize.AtlasResourceTypes;
import org.apache.atlas.utils.PropertiesUtil;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOCase;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public final class SimpleAtlasAuthorizer implements AtlasAuthorizer {

	public enum AtlasAccessorTypes {
        USER, GROUP;
    }

    private static final Logger LOG = LoggerFactory.getLogger(SimpleAtlasAuthorizer.class);
    private boolean isDebugEnabled = LOG.isDebugEnabled();
    private final static String WILDCARD_ASTERISK = "*";
    private final static String WILDCARDS = "*?";
    private boolean optIgnoreCase = false;

    private Map<String, Map<AtlasResourceTypes, List<String>>> userReadMap = null;
    private Map<String, Map<AtlasResourceTypes, List<String>>> userWriteMap = null;
    private Map<String, Map<AtlasResourceTypes, List<String>>> userUpdateMap = null;
    private Map<String, Map<AtlasResourceTypes, List<String>>> userDeleteMap = null;
    private Map<String, Map<AtlasResourceTypes, List<String>>> groupReadMap = null;
    private Map<String, Map<AtlasResourceTypes, List<String>>> groupWriteMap = null;
    private Map<String, Map<AtlasResourceTypes, List<String>>> groupUpdateMap = null;
    private Map<String, Map<AtlasResourceTypes, List<String>>> groupDeleteMap = null;

    public SimpleAtlasAuthorizer() {
    }
    

    @Override
    public void init() {
        if (isDebugEnabled) {
            LOG.debug("==> SimpleAtlasAuthorizer init");
        }
        try {

            PolicyUtil util = new PolicyUtil();
            PolicyParser parser = new PolicyParser();
            optIgnoreCase = Boolean.valueOf(PropertiesUtil.getProperty("optIgnoreCase", "false"));

            if (isDebugEnabled) {
                LOG.debug("Read from PropertiesUtil --> optIgnoreCase :: " + optIgnoreCase);
            }

            Configuration configuration = ApplicationProperties.get();
            String policyStorePath = configuration.getString("atlas.auth.policy.file", System.getProperty("atlas.conf")+"/policy-store.txt");

            if (isDebugEnabled) {
                LOG.debug("Loading Apache Atlas policies from : " + policyStorePath);
            }

            List<String> policies = FileReaderUtil.readFile(policyStorePath);
            List<PolicyDef> policyDef = parser.parsePolicies(policies);

            userReadMap = util.createPermissionMap(policyDef, AtlasActionTypes.READ, AtlasAccessorTypes.USER);
            userWriteMap = util.createPermissionMap(policyDef, AtlasActionTypes.CREATE, AtlasAccessorTypes.USER);
            userUpdateMap = util.createPermissionMap(policyDef, AtlasActionTypes.UPDATE, AtlasAccessorTypes.USER);
            userDeleteMap = util.createPermissionMap(policyDef, AtlasActionTypes.DELETE, AtlasAccessorTypes.USER);

            groupReadMap = util.createPermissionMap(policyDef, AtlasActionTypes.READ, AtlasAccessorTypes.GROUP);
            groupWriteMap = util.createPermissionMap(policyDef, AtlasActionTypes.CREATE, AtlasAccessorTypes.GROUP);
            groupUpdateMap = util.createPermissionMap(policyDef, AtlasActionTypes.UPDATE, AtlasAccessorTypes.GROUP);
            groupDeleteMap = util.createPermissionMap(policyDef, AtlasActionTypes.DELETE, AtlasAccessorTypes.GROUP);

            if (isDebugEnabled) {
                LOG.debug("\n\nUserReadMap :: " + userReadMap + "\nGroupReadMap :: " + groupReadMap);
                LOG.debug("\n\nUserWriteMap :: " + userWriteMap + "\nGroupWriteMap :: " + groupWriteMap);
                LOG.debug("\n\nUserUpdateMap :: " + userUpdateMap + "\nGroupUpdateMap :: " + groupUpdateMap);
                LOG.debug("\n\nUserDeleteMap :: " + userDeleteMap + "\nGroupDeleteMap :: " + groupDeleteMap);
            }

        } catch (IOException | AtlasException e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("SimpleAtlasAuthorizer could not be initialized properly due to : ", e);
            }
        }
    }

    @Override
    public boolean isAccessAllowed(AtlasAccessRequest request) throws AtlasAuthorizationException {
        if (isDebugEnabled) {
            LOG.debug("==> SimpleAtlasAuthorizer isAccessAllowed");
            LOG.debug("isAccessAllowd(" + request + ")");
        }
        String user = request.getUser();
        Set<String> groups = request.getUserGroups();
        AtlasActionTypes action = request.getAction();
        String resource = request.getResource();
        Set<AtlasResourceTypes> resourceTypes = request.getResourceTypes();
        if (isDebugEnabled)
            LOG.debug("Checking for :: \nUser :: " + user + "\nGroups :: " + groups + "\nAction :: " + action
                + "\nResource :: " + resource);

        boolean isAccessAllowed = false;
        boolean isUser = user == null ? false : true;
        boolean isGroup = groups == null ? false : true;

        if ((!isUser && !isGroup) || action == null || resource == null) {
            if (isDebugEnabled) {
                LOG.debug("Please check the formation AtlasAccessRequest.");
            }
            return isAccessAllowed;
        } else {
            if (isDebugEnabled) {
                LOG.debug("checkAccess for Operation :: " + action + " on Resource " + resourceTypes + ":" + resource);
            }
            switch (action) {
                case READ:
                    isAccessAllowed = checkAccess(user, resourceTypes, resource, userReadMap);
                    isAccessAllowed =
                        isAccessAllowed == false ? checkAccessForGroups(groups, resourceTypes, resource, groupReadMap)
                            : isAccessAllowed;
                    break;
                case CREATE:
                    isAccessAllowed = checkAccess(user, resourceTypes, resource, userWriteMap);
                    isAccessAllowed =
                        isAccessAllowed == false ? checkAccessForGroups(groups, resourceTypes, resource, groupWriteMap)
                            : isAccessAllowed;
                    break;
                case UPDATE:
                    isAccessAllowed = checkAccess(user, resourceTypes, resource, userUpdateMap);
                    isAccessAllowed =
                        isAccessAllowed == false
                            ? checkAccessForGroups(groups, resourceTypes, resource, groupUpdateMap) : isAccessAllowed;
                    break;
                case DELETE:
                    isAccessAllowed = checkAccess(user, resourceTypes, resource, userDeleteMap);
                    isAccessAllowed =
                        isAccessAllowed == false
                            ? checkAccessForGroups(groups, resourceTypes, resource, groupDeleteMap) : isAccessAllowed;
                    break;
                default:
                    if (isDebugEnabled) {
                        LOG.debug("Invalid Action " + action+"\nRaising AtlasAuthorizationException!!!");
                    }
                    throw new AtlasAuthorizationException("Invalid Action :: " + action);
            }
        }

        if (isDebugEnabled) {
            LOG.debug("<== SimpleAtlasAuthorizer isAccessAllowed = " + isAccessAllowed);
        }

        return isAccessAllowed;
    }

    private boolean checkAccess(String accessor, Set<AtlasResourceTypes> resourceTypes, String resource,
        Map<String, Map<AtlasResourceTypes, List<String>>> map) {
        if (isDebugEnabled) {
            LOG.debug("==> SimpleAtlasAuthorizer checkAccess");
            LOG.debug("Now checking access for accessor : " + accessor + "\nResource Types : " + resourceTypes
                + "\nResource : " + resource + "\nMap : " + map);
        }
        boolean result = true;
        Map<AtlasResourceTypes, List<String>> rescMap = map.get(accessor);
        if (rescMap != null) {
            for (AtlasResourceTypes resourceType : resourceTypes) {
                List<String> accessList = rescMap.get(resourceType);
                if (isDebugEnabled) {
                    LOG.debug("\nChecking for resource : " + resource + " in list : " + accessList + "\n");
                }
                if (accessList != null) {
                    result = result && isMatch(resource, accessList);
                } else {
                    result = false;
                }
            }
        } else {
            result = false;
            if (isDebugEnabled)
                LOG.debug("Key " + accessor + " missing. Returning with result : " + result);
        }

        if (isDebugEnabled) {
            LOG.debug("Check for " + accessor + " :: " + result);
            LOG.debug("<== SimpleAtlasAuthorizer checkAccess");
        }
        return result;
    }

    private boolean checkAccessForGroups(Set<String> groups, Set<AtlasResourceTypes> resourceType, String resource,
        Map<String, Map<AtlasResourceTypes, List<String>>> map) {
        boolean isAccessAllowed = false;
        if (isDebugEnabled) {
            LOG.debug("==> SimpleAtlasAuthorizer checkAccessForGroups");
        }

        for (String group : groups) {
            isAccessAllowed = checkAccess(group, resourceType, resource, map);
            if (isAccessAllowed) {
                break;
            }
        }

        if (isDebugEnabled) {
            LOG.debug("<== SimpleAtlasAuthorizer checkAccessForGroups");
        }
        return isAccessAllowed;
    }

    private boolean resourceMatchHelper(List<String> policyResource) {
        boolean isMatchAny = false;
        if (isDebugEnabled) {
            LOG.debug("==> SimpleAtlasAuthorizer resourceMatchHelper");
        }

        boolean optWildCard = true;

        List<String> policyValues = new ArrayList<String>();

        if (policyResource != null) {
            boolean isWildCardPresent = !optWildCard;
            for (String policyValue : policyResource) {
                if (StringUtils.isEmpty(policyValue)) {
                    continue;
                }
                if (StringUtils.containsOnly(policyValue, WILDCARD_ASTERISK)) {
                    isMatchAny = true;
                } else if (!isWildCardPresent && StringUtils.containsAny(policyValue, WILDCARDS)) {
                    isWildCardPresent = true;
                }
                policyValues.add(policyValue);
            }
            optWildCard = optWildCard && isWildCardPresent;
        } else {
            isMatchAny = false;
        }

        if (isDebugEnabled) {
            LOG.debug("<== SimpleAtlasAuthorizer resourceMatchHelper");
        }
        return isMatchAny;
    }

    private boolean isMatch(String resource, List<String> policyValues) {
        if (isDebugEnabled) {
            LOG.debug("==> SimpleAtlasAuthorizer isMatch");
        }
        boolean isMatchAny = resourceMatchHelper(policyValues);
        boolean isMatch = false;
        boolean allValuesRequested = isAllValuesRequested(resource);

        if (allValuesRequested || isMatchAny) {
            isMatch = isMatchAny;
        } else {
            for (String policyValue : policyValues) {
                if (policyValue.contains("*")) {
                    isMatch =
                        optIgnoreCase ? FilenameUtils.wildcardMatch(resource, policyValue, IOCase.INSENSITIVE)
                            : FilenameUtils.wildcardMatch(resource, policyValue, IOCase.SENSITIVE);
                } else {
                    isMatch =
                        optIgnoreCase ? StringUtils.equalsIgnoreCase(resource, policyValue) : StringUtils.equals(
                            resource, policyValue);
                }
                if (isMatch) {
                    break;
                }
            }
        }

        if (isMatch == false) {

            if (isDebugEnabled) {
                StringBuilder sb = new StringBuilder();
                sb.append("[");
                for (String policyValue : policyValues) {
                    sb.append(policyValue);
                    sb.append(" ");
                }
                sb.append("]");

                LOG.debug("AtlasDefaultResourceMatcher.isMatch returns FALSE, (resource=" + resource
                    + ", policyValues=" + sb.toString() + ")");
            }

        }

        if (isDebugEnabled) {
            LOG.debug("<== SimpleAtlasAuthorizer isMatch(" + resource + "): " + isMatch);
        }

        return isMatch;
    }

    private boolean isAllValuesRequested(String resource) {
        boolean result = StringUtils.isEmpty(resource) || WILDCARD_ASTERISK.equals(resource);
        return result;
    }

    @Override
    public void cleanUp() {
        if (isDebugEnabled) {
            LOG.debug("==> +SimpleAtlasAuthorizer cleanUp");
        }
        userReadMap = null;
        userWriteMap = null;
        userUpdateMap = null;
        userDeleteMap = null;
        groupReadMap = null;
        groupWriteMap = null;
        groupUpdateMap = null;
        groupDeleteMap = null;
        if (isDebugEnabled) {
            LOG.debug("<== +SimpleAtlasAuthorizer cleanUp");
        }
    }

    /*
     * NOTE :: This method is added for setting the maps for testing purpose.
     */
    @VisibleForTesting
    public void setResourcesForTesting(Map<String, Map<AtlasResourceTypes, List<String>>> userMap,
        Map<String, Map<AtlasResourceTypes, List<String>>> groupMap, AtlasActionTypes actionTypes) {

        switch (actionTypes) {
            case READ:
                this.userReadMap = userMap;
                this.groupReadMap = groupMap;
                break;

            case CREATE:

                this.userWriteMap = userMap;
                this.groupWriteMap = groupMap;
                break;
            case UPDATE:

                this.userUpdateMap = userMap;
                this.groupUpdateMap = groupMap;
                break;
            case DELETE:

                this.userDeleteMap = userMap;
                this.groupDeleteMap = groupMap;
                break;

            default:
                if (isDebugEnabled) {
                    LOG.debug("No such action available");
                }
                break;
        }
    }
    
}


