/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;

import org.apache.atlas.authorize.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class SimpleAtlasAuthorizerTest {

    private static Logger LOG = LoggerFactory
            .getLogger(SimpleAtlasAuthorizerTest.class);

    @Test
    public void testAccessAllowedForUserAndGroup() {

        Map<String, Map<AtlasResourceTypes, List<String>>> userReadMap = null;
        Map<String, Map<AtlasResourceTypes, List<String>>> groupReadMap = null;
        List<String> policies = new ArrayList<String>();
        policies.add("hivePolicy;;usr1:r,usr2:rw;;grp1:rwu,grp2:u;;type:*abc,type:PII");

        List<PolicyDef> policyDefs = new PolicyParser().parsePolicies(policies);
        PolicyUtil policyUtil = new PolicyUtil();
        // group read map
        groupReadMap = policyUtil.createPermissionMap(policyDefs,
                AtlasActionTypes.READ, SimpleAtlasAuthorizer.AtlasAccessorTypes.GROUP);
        // creating user readMap
        userReadMap = policyUtil.createPermissionMap(policyDefs,
                AtlasActionTypes.READ, SimpleAtlasAuthorizer.AtlasAccessorTypes.USER);

        Set<AtlasResourceTypes> resourceType = new HashSet<AtlasResourceTypes>();
        resourceType.add(AtlasResourceTypes.TYPE);
        String resource = "xsdfhjabc";
        AtlasActionTypes action = AtlasActionTypes.READ;
        String user = "usr1";

        Set<String> userGroups = new HashSet<String>();
        userGroups.add("grp3");
        try {
            AtlasAccessRequest request = new AtlasAccessRequest(resourceType,
                    resource, action, user, userGroups);
            SimpleAtlasAuthorizer authorizer = (SimpleAtlasAuthorizer) AtlasAuthorizerFactory
                    .getAtlasAuthorizer();

            authorizer
                    .setResourcesForTesting(userReadMap, groupReadMap, action);

            boolean isAccessAllowed = authorizer.isAccessAllowed(request);
            // getUserReadMap
            AssertJUnit.assertEquals(true, isAccessAllowed);
        } catch (AtlasAuthorizationException e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("AtlasAuthorizationException in Unit Test", e);
            }
        }

    }

    @Test
    public void testAccessAllowedForGroup() {

        Map<String, Map<AtlasResourceTypes, List<String>>> userReadMap = null;
        Map<String, Map<AtlasResourceTypes, List<String>>> groupReadMap = null;
        List<String> policies = new ArrayList<String>();
        policies.add("hivePolicy;;usr1:r,usr2:rw;;grp1:rwu,grp2:u;;type:PII");

        List<PolicyDef> policyDefs = new PolicyParser().parsePolicies(policies);
        PolicyUtil policyUtil = new PolicyUtil();
        // creating group read map
        groupReadMap = policyUtil.createPermissionMap(policyDefs,
                AtlasActionTypes.READ, SimpleAtlasAuthorizer.AtlasAccessorTypes.GROUP);
        // creating user readMap
        userReadMap = policyUtil.createPermissionMap(policyDefs,
                AtlasActionTypes.READ, SimpleAtlasAuthorizer.AtlasAccessorTypes.USER);

        Set<AtlasResourceTypes> resourceType = new HashSet<AtlasResourceTypes>();
        resourceType.add(AtlasResourceTypes.TYPE);
        String resource = "PII";
        AtlasActionTypes action = AtlasActionTypes.READ;
        String user = "usr3";
        Set<String> userGroups = new HashSet<String>();
        userGroups.add("grp1");
        AtlasAccessRequest request = new AtlasAccessRequest(resourceType,
                resource, action, user, userGroups);
        try {
            SimpleAtlasAuthorizer authorizer = (SimpleAtlasAuthorizer) AtlasAuthorizerFactory
                    .getAtlasAuthorizer();
            authorizer
                    .setResourcesForTesting(userReadMap, groupReadMap, action);

            boolean isAccessAllowed = authorizer.isAccessAllowed(request);
            AssertJUnit.assertEquals(true, isAccessAllowed);
        } catch (AtlasAuthorizationException e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("AtlasAuthorizationException in Unit Test", e);
            }

        }

    }

    @Test
    public void testResourceNotAvailableInPolicy() {

        Map<String, Map<AtlasResourceTypes, List<String>>> userReadMap = null;
        Map<String, Map<AtlasResourceTypes, List<String>>> groupReadMap = null;
        List<String> policies = new ArrayList<String>();
        policies.add("hivePolicy;;usr1:r,usr2:rw;;grp1:rwu,grp2:u;;type:PII");

        List<PolicyDef> policyDefs = new PolicyParser().parsePolicies(policies);
        PolicyUtil policyUtil = new PolicyUtil();
        // group read map
        groupReadMap = policyUtil.createPermissionMap(policyDefs,
                AtlasActionTypes.READ, SimpleAtlasAuthorizer.AtlasAccessorTypes.GROUP);
        // creating user readMap
        userReadMap = policyUtil.createPermissionMap(policyDefs,
                AtlasActionTypes.READ, SimpleAtlasAuthorizer.AtlasAccessorTypes.USER);

        Set<AtlasResourceTypes> resourceType = new HashSet<AtlasResourceTypes>();
        resourceType.add(AtlasResourceTypes.TYPE);
        String resource = "abc";
        AtlasActionTypes action = AtlasActionTypes.READ;
        String user = "usr1";
        Set<String> userGroups = new HashSet<String>();
        userGroups.add("grp1");
        AtlasAccessRequest request = new AtlasAccessRequest(resourceType,
                resource, action, user, userGroups);
        try {
            SimpleAtlasAuthorizer authorizer = (SimpleAtlasAuthorizer) AtlasAuthorizerFactory
                    .getAtlasAuthorizer();
            authorizer
                    .setResourcesForTesting(userReadMap, groupReadMap, action);

            boolean isAccessAllowed = authorizer.isAccessAllowed(request);
            AssertJUnit.assertEquals(false, isAccessAllowed);
        } catch (AtlasAuthorizationException e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("AtlasAuthorizationException in Unit Test", e);
            }
        }

    }

    @Test
    public void testAccessNotAllowedForUserAndGroup() {

        Map<String, Map<AtlasResourceTypes, List<String>>> userReadMap = null;
        Map<String, Map<AtlasResourceTypes, List<String>>> groupReadMap = null;
        List<String> policies = new ArrayList<String>();
        policies.add("hivePolicy;;usr1:r,usr2:rw;;grp1:rwu,grp2:u;;type:PII");

        List<PolicyDef> policyDefs = new PolicyParser().parsePolicies(policies);
        PolicyUtil policyUtil = new PolicyUtil();
        // group read map
        groupReadMap = policyUtil.createPermissionMap(policyDefs,
                AtlasActionTypes.READ, SimpleAtlasAuthorizer.AtlasAccessorTypes.GROUP);
        // creating user readMap
        userReadMap = policyUtil.createPermissionMap(policyDefs,
                AtlasActionTypes.READ, SimpleAtlasAuthorizer.AtlasAccessorTypes.USER);

        Set<AtlasResourceTypes> resourceType = new HashSet<AtlasResourceTypes>();
        resourceType.add(AtlasResourceTypes.TYPE);
        String resource = "PII";
        AtlasActionTypes action = AtlasActionTypes.READ;
        String user = "usr3";
        Set<String> userGroups = new HashSet<String>();
        userGroups.add("grp3");
        AtlasAccessRequest request = new AtlasAccessRequest(resourceType,
                resource, action, user, userGroups);
        try {
            SimpleAtlasAuthorizer authorizer = (SimpleAtlasAuthorizer) AtlasAuthorizerFactory
                    .getAtlasAuthorizer();
            authorizer
                    .setResourcesForTesting(userReadMap, groupReadMap, action);

            boolean isAccessAllowed = authorizer.isAccessAllowed(request);
            AssertJUnit.assertEquals(false, isAccessAllowed);
        } catch (AtlasAuthorizationException e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("AtlasAuthorizationException in Unit Test", e);
            }
        }

    }

}
