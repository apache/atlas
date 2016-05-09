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

import java.util.List;
import java.util.Map;

public class PolicyDef {

    private String policyName;
    private Map<String, List<AtlasActionTypes>> users;
    private Map<String, List<AtlasActionTypes>> groups;
    private Map<AtlasResourceTypes, List<String>> resources;

    public String getPolicyName() {
        return policyName;
    }

    public void setPolicyName(String policyName) {
        this.policyName = policyName;
    }

    public Map<String, List<AtlasActionTypes>> getUsers() {
        return users;
    }

    public void setUsers(Map<String, List<AtlasActionTypes>> users) {
        this.users = users;
    }

    public Map<String, List<AtlasActionTypes>> getGroups() {
        return groups;
    }

    public void setGroups(Map<String, List<AtlasActionTypes>> groups) {
        this.groups = groups;
    }

    public Map<AtlasResourceTypes, List<String>> getResources() {
        return resources;
    }

    public void setResources(Map<AtlasResourceTypes, List<String>> resources) {
        this.resources = resources;
    }

    @Override
    public String toString() {
        return "PolicyDef [policyName=" + policyName + ", users=" + users + ", groups=" + groups + ", resources="
            + resources + "]";
    }

}
