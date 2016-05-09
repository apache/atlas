/** Licensed to the Apache Software Foundation (ASF) under one
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
package org.apache.atlas.authorize;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PolicyUtil {

    private static Logger LOG = LoggerFactory.getLogger(PolicyUtil.class);
    private static boolean isDebugEnabled = LOG.isDebugEnabled();
    private Map<String, Map<AtlasResourceTypes, List<String>>> userReadMap;
    private Map<String, Map<AtlasResourceTypes, List<String>>> userWriteMap;
    private Map<String, Map<AtlasResourceTypes, List<String>>> userUpdateMap;
    private Map<String, Map<AtlasResourceTypes, List<String>>> userDeleteMap;

    private Map<String, Map<AtlasResourceTypes, List<String>>> groupReadMap;
    private Map<String, Map<AtlasResourceTypes, List<String>>> groupWriteMap;
    private Map<String, Map<AtlasResourceTypes, List<String>>> groupUpdateMap;
    private Map<String, Map<AtlasResourceTypes, List<String>>> groupDeleteMap;

    /**
     * @return the userReadMap
     */
    public Map<String, Map<AtlasResourceTypes, List<String>>> getUserReadMap() {
        return userReadMap;
    }

    /**
     * @return the userWriteMap
     */
    public Map<String, Map<AtlasResourceTypes, List<String>>> getUserWriteMap() {
        return userWriteMap;
    }

    /**
     * @return the userUpdateMap
     */
    public Map<String, Map<AtlasResourceTypes, List<String>>> getUserUpdateMap() {
        return userUpdateMap;
    }

    /**
     * @return the userDeleteMap
     */
    public Map<String, Map<AtlasResourceTypes, List<String>>> getUserDeleteMap() {
        return userDeleteMap;
    }

    /**
     * @return the groupReadMap
     */
    public Map<String, Map<AtlasResourceTypes, List<String>>> getGroupReadMap() {
        return groupReadMap;
    }

    /**
     * @return the groupWriteMap
     */
    public Map<String, Map<AtlasResourceTypes, List<String>>> getGroupWriteMap() {
        return groupWriteMap;
    }

    /**
     * @return the groupUpdateMap
     */
    public Map<String, Map<AtlasResourceTypes, List<String>>> getGroupUpdateMap() {
        return groupUpdateMap;
    }

    /**
     * @return the groupDeleteMap
     */
    public Map<String, Map<AtlasResourceTypes, List<String>>> getGroupDeleteMap() {
        return groupDeleteMap;
    }

    public Map<String, Map<AtlasResourceTypes, List<String>>> createPermissionMap(List<PolicyDef> policyDefList,
        AtlasActionTypes permissionType, AtlasAccessorTypes principalType) {
        if (isDebugEnabled) {
            LOG.debug("<== PolicyUtil createPermissionMap");
            LOG.debug("Creating Permission Map for :: " + permissionType + " & " + principalType);
        }
        Map<String, Map<AtlasResourceTypes, List<String>>> userReadMap =
            new HashMap<String, Map<AtlasResourceTypes, List<String>>>();

        // Iterate over the list of policies to create map
        for (PolicyDef policyDef : policyDefList) {
            LOG.info("Processing policy def : " + policyDef);
            Map<String, List<AtlasActionTypes>> principalMap =
                principalType.equals(AtlasAccessorTypes.USER) ? policyDef.getUsers() : policyDef.getGroups();
            // For every policy extract the resource list and populate the user map
            for (Entry<String, List<AtlasActionTypes>> e : principalMap.entrySet()) {
                // Check if the user has passed permission type like READ
                if (!e.getValue().contains(permissionType)) {
                    continue;
                }
                // See if the current user is already added to map
                String username = e.getKey();
                Map<AtlasResourceTypes, List<String>> userResourceList = userReadMap.get(username);

                // If its not added then create a new resource list
                if (userResourceList == null) {
                    if (isDebugEnabled) {
                        LOG.debug("Resource list not found for " + username + ", creating it");
                    }
                    userResourceList = new HashMap<AtlasResourceTypes, List<String>>();
                }
                /*
                 * Iterate over resources from the current policy def and update the resource list for the current user
                 */
                for (Entry<AtlasResourceTypes, List<String>> resourceTypeMap : policyDef.getResources().entrySet()) {
                    // For the current resourceType in the policyDef, get the
                    // current list of resources already added
                    AtlasResourceTypes type = resourceTypeMap.getKey();
                    List<String> resourceList = userResourceList.get(type);

                    if (resourceList == null) {
                        // if the resource list was not added for this type then
                        // create and add all the resources in this policy
                        resourceList = new ArrayList<String>();
                        resourceList.addAll(resourceTypeMap.getValue());
                    } else {
                        // if the resource list is present then merge both the
                        // list
                        resourceList.removeAll(resourceTypeMap.getValue());
                        resourceList.addAll(resourceTypeMap.getValue());
                    }

                    userResourceList.put(type, resourceList);
                }
                userReadMap.put(username, userResourceList);
                LOG.info("userReadMap=====>>>>>> " + userReadMap);
            }
        }
        if (isDebugEnabled) {
            LOG.debug("Returning Map for " + principalType + " :: " + userReadMap);
            LOG.debug("==> PolicyUtil createPermissionMap");
        }
        return userReadMap;

    }

}
