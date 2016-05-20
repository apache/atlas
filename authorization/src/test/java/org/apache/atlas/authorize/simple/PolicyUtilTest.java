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

import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.atlas.authorize.simple.SimpleAtlasAuthorizer;
import org.apache.atlas.authorize.AtlasActionTypes;
import org.apache.atlas.authorize.AtlasResourceTypes;
import org.apache.atlas.authorize.simple.PolicyDef;
import org.apache.atlas.authorize.simple.PolicyParser;
import org.apache.atlas.authorize.simple.PolicyUtil;
import org.testng.annotations.Test;

public class PolicyUtilTest {

    @Test
    public void testCreatePermissionMap() {

        HashMap<AtlasResourceTypes, List<String>> resourceMap = new HashMap<AtlasResourceTypes, List<String>>();
        List<String> resource1List = new ArrayList<String>();
        resource1List.add("*abc");
        resourceMap.put(AtlasResourceTypes.ENTITY, resource1List);

        List<String> resource2List = new ArrayList<String>();
        resource2List.add("*xyz");
        resourceMap.put(AtlasResourceTypes.OPERATION, resource2List);

        List<String> resource3List = new ArrayList<String>();
        resource3List.add("PII");
        resourceMap.put(AtlasResourceTypes.TYPE, resource3List);

        Map<String, HashMap<AtlasResourceTypes, List<String>>> permissionMap =
            new HashMap<String, HashMap<AtlasResourceTypes, List<String>>>();
        permissionMap.put("grp1", resourceMap);

        List<String> policies = new ArrayList<String>();
        policies.add("hivePolicy;;usr1:r,usr2:rw;;grp1:rwu,grp2:u;;entity:*abc,operation:*xyz,type:PII");
        List<PolicyDef> policyDefList = new PolicyParser().parsePolicies(policies);

        Map<String, Map<AtlasResourceTypes, List<String>>> createdPermissionMap =
            new PolicyUtil().createPermissionMap(policyDefList, AtlasActionTypes.READ, SimpleAtlasAuthorizer.AtlasAccessorTypes.GROUP);

        assertEquals(permissionMap, createdPermissionMap);

    }

    @Test
    public void testMergeCreatePermissionMap() {

        HashMap<AtlasResourceTypes, List<String>> resourceMap = new HashMap<AtlasResourceTypes, List<String>>();
        List<String> resource1List = new ArrayList<String>();
        resource1List.add("*abc");
        resourceMap.put(AtlasResourceTypes.ENTITY, resource1List);

        List<String> resource2List = new ArrayList<String>();
        resource2List.add("*x");
        resource2List.add("*xyz");
        resourceMap.put(AtlasResourceTypes.OPERATION, resource2List);

        List<String> resource3List = new ArrayList<String>();
        resource3List.add("PII");
        resourceMap.put(AtlasResourceTypes.TYPE, resource3List);

        Map<String, HashMap<AtlasResourceTypes, List<String>>> permissionMap =
            new HashMap<String, HashMap<AtlasResourceTypes, List<String>>>();
        permissionMap.put("grp1", resourceMap);

        List<String> policies = new ArrayList<String>();
        policies.add("hivePolicys;;;;grp1:rwu;;entity:*abc,operation:*xyz,operation:*x");
        policies.add("hivePolicy;;;;grp1:rwu;;entity:*abc,operation:*xyz");
        policies.add("hivePolicy;;usr1:r,usr2:rw;;grp1:rwu;;entity:*abc,operation:*xyz");
        policies.add("hivePolicy;;usr1:r,usr2:rw;;grp1:rwu,grp2:u;;entity:*abc,operation:*xyz,type:PII");
        List<PolicyDef> policyDefList = new PolicyParser().parsePolicies(policies);

        Map<String, Map<AtlasResourceTypes, List<String>>> createdPermissionMap =
            new PolicyUtil().createPermissionMap(policyDefList, AtlasActionTypes.READ, SimpleAtlasAuthorizer.AtlasAccessorTypes.GROUP);

        assertEquals(permissionMap, createdPermissionMap);

    }
}
