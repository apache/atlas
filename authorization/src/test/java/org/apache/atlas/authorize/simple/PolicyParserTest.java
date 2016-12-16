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

import org.apache.atlas.authorize.AtlasActionTypes;
import org.apache.atlas.authorize.AtlasResourceTypes;
import org.apache.atlas.authorize.simple.PolicyDef;
import org.apache.atlas.authorize.simple.PolicyParser;
import org.testng.annotations.Test;

public class PolicyParserTest {

    @Test
    public void testParsePoliciesWithAllProperties() {
        List<String> policies = new ArrayList<>();
        policies.add("hivePolicy;;usr1:r,usr2:rw;;grp1:rwu,grp2:u;;entity:*abc,operation:*xyz,type:PII");
        /* Creating group data */
        Map<String, List<AtlasActionTypes>> groupMap = new HashMap<>();
        List<AtlasActionTypes> accessList1 = new ArrayList<>();
        accessList1.add(AtlasActionTypes.READ);
        accessList1.add(AtlasActionTypes.CREATE);
        accessList1.add(AtlasActionTypes.UPDATE);

        groupMap.put("grp1", accessList1);
        List<AtlasActionTypes> accessList2 = new ArrayList<>();
        accessList2.add(AtlasActionTypes.UPDATE);
        groupMap.put("grp2", accessList2);

        /* Creating user data */
        Map<String, List<AtlasActionTypes>> usersMap = new HashMap<>();
        List<AtlasActionTypes> usr1AccessList = new ArrayList<>();
        usr1AccessList.add(AtlasActionTypes.READ);
        usersMap.put("usr1", usr1AccessList);

        List<AtlasActionTypes> usr2AccessList = new ArrayList<>();
        usr2AccessList.add(AtlasActionTypes.READ);
        usr2AccessList.add(AtlasActionTypes.CREATE);
        usersMap.put("usr2", usr2AccessList);

        /* Creating resources data */
        Map<AtlasResourceTypes, List<String>> resourceMap = new HashMap<>();
        List<String> resource1List = new ArrayList<>();
        resource1List.add("*abc");
        resourceMap.put(AtlasResourceTypes.ENTITY, resource1List);

        List<String> resource2List = new ArrayList<>();
        resource2List.add("*xyz");
        resourceMap.put(AtlasResourceTypes.OPERATION, resource2List);

        List<String> resource3List = new ArrayList<>();
        resource3List.add("PII");
        resourceMap.put(AtlasResourceTypes.TYPE, resource3List);

        List<PolicyDef> policyDefs = new PolicyParser().parsePolicies(policies);
        for (PolicyDef def : policyDefs) {

            assertEquals(def.getPolicyName(), "hivePolicy");
            assertEquals(def.getGroups(), groupMap);
            assertEquals(def.getUsers(), usersMap);
            assertEquals(def.getResources(), resourceMap);

        }

    }

    @Test
    public void testParsePoliciesWithOutUserProperties() {
        List<String> policies = new ArrayList<>();
        policies.add("hivePolicy;;;;grp1:rwu,grp2:u;;entity:*abc,operation:*xyz,type:PII");
        // Creating group data
        Map<String, List<AtlasActionTypes>> groupMap = new HashMap<>();
        List<AtlasActionTypes> accessList1 = new ArrayList<>();
        accessList1.add(AtlasActionTypes.READ);
        accessList1.add(AtlasActionTypes.CREATE);
        accessList1.add(AtlasActionTypes.UPDATE);

        groupMap.put("grp1", accessList1);
        List<AtlasActionTypes> accessList2 = new ArrayList<>();
        accessList2.add(AtlasActionTypes.UPDATE);
        groupMap.put("grp2", accessList2);

        // Creating user data
        Map<String, List<AtlasActionTypes>> usersMap = new HashMap<>();

        // Creating resources data
        Map<AtlasResourceTypes, List<String>> resourceMap = new HashMap<>();
        List<String> resource1List = new ArrayList<>();
        resource1List.add("*abc");
        resourceMap.put(AtlasResourceTypes.ENTITY, resource1List);

        List<String> resource2List = new ArrayList<>();
        resource2List.add("*xyz");
        resourceMap.put(AtlasResourceTypes.OPERATION, resource2List);

        List<String> resource3List = new ArrayList<>();
        resource3List.add("PII");
        resourceMap.put(AtlasResourceTypes.TYPE, resource3List);

        List<PolicyDef> policyDefs = new PolicyParser().parsePolicies(policies);
        for (PolicyDef def : policyDefs) {

            assertEquals(def.getPolicyName(), "hivePolicy");
            assertEquals(def.getGroups(), groupMap);
            assertEquals(def.getUsers(), usersMap);
            assertEquals(def.getResources(), resourceMap);

        }

    }

    @Test
    public void testParsePoliciesWithOutGroupProperties() {
        List<String> policies = new ArrayList<>();
        policies.add("hivePolicy;;usr1:r,usr2:rw;;;;entity:*abc,operation:*xyz,type:PII");
        // Creating group data
        Map<String, List<AtlasActionTypes>> groupMap = new HashMap<>();

        // Creating user data
        Map<String, List<AtlasActionTypes>> usersMap = new HashMap<>();
        List<AtlasActionTypes> usr1AccessList = new ArrayList<>();
        usr1AccessList.add(AtlasActionTypes.READ);
        usersMap.put("usr1", usr1AccessList);

        List<AtlasActionTypes> usr2AccessList = new ArrayList<>();
        usr2AccessList.add(AtlasActionTypes.READ);
        usr2AccessList.add(AtlasActionTypes.CREATE);
        usersMap.put("usr2", usr2AccessList);

        // Creating resources data
        Map<AtlasResourceTypes, List<String>> resourceMap = new HashMap<>();
        List<String> resource1List = new ArrayList<>();
        resource1List.add("*abc");
        resourceMap.put(AtlasResourceTypes.ENTITY, resource1List);

        List<String> resource2List = new ArrayList<>();
        resource2List.add("*xyz");
        resourceMap.put(AtlasResourceTypes.OPERATION, resource2List);

        List<String> resource3List = new ArrayList<>();
        resource3List.add("PII");
        resourceMap.put(AtlasResourceTypes.TYPE, resource3List);

        List<PolicyDef> policyDefs = new PolicyParser().parsePolicies(policies);
        for (PolicyDef def : policyDefs) {
            assertEquals(def.getPolicyName(), "hivePolicy");
            assertEquals(def.getGroups(), groupMap);
            assertEquals(def.getUsers(), usersMap);
            assertEquals(def.getResources(), resourceMap);
        }
    }
}