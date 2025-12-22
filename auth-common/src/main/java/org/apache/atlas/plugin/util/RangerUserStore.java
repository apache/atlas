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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.atlas.plugin.model.GroupInfo;
import org.apache.atlas.plugin.model.UserInfo;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@JsonInclude(JsonInclude.Include.NON_NULL)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class RangerUserStore implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final String CLOUD_IDENTITY_NAME = "cloud_id";

    private Long                             userStoreVersion;
    private Date                             userStoreUpdateTime;
    private Map<String, Map<String, String>> userAttrMapping;
    private Map<String, Map<String, String>> groupAttrMapping ;
    private Map<String, Set<String>>         userGroupMapping;
    private Map<String, String>              userCloudIdMapping;
    private Map<String, String>              groupCloudIdMapping;
    private String                           serviceName;

    public RangerUserStore() {this(-1L, null, null, null);}

    public RangerUserStore(Long userStoreVersion, Set<UserInfo> users, Set<GroupInfo> groups, Map<String, Set<String>> userGroups) {
        setUserStoreVersion(userStoreVersion);
        setUserStoreUpdateTime(new Date());
        setUserGroupMapping(userGroups);
        buildMap(users, groups);
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public Long getUserStoreVersion() {
        return userStoreVersion;
    }

    public void setUserStoreVersion(Long userStoreVersion) {
        this.userStoreVersion = userStoreVersion;
    }

    public Date getUserStoreUpdateTime() {
        return userStoreUpdateTime;
    }

    public void setUserStoreUpdateTime(Date userStoreUpdateTime) {
        this.userStoreUpdateTime = userStoreUpdateTime;
    }

    public Map<String, Map<String, String>> getUserAttrMapping() {
        return userAttrMapping;
    }

    public void setUserAttrMapping(Map<String, Map<String, String>> userAttrMapping) {
        this.userAttrMapping = userAttrMapping;
    }

    public Map<String, Map<String, String>> getGroupAttrMapping() {
        return groupAttrMapping;
    }

    public void setGroupAttrMapping(Map<String, Map<String, String>> groupAttrMapping) {
        this.groupAttrMapping = groupAttrMapping;
    }

    public Map<String, Set<String>> getUserGroupMapping() {
        return userGroupMapping;
    }

    public void setUserGroupMapping(Map<String, Set<String>> userGroupMapping) {
        this.userGroupMapping = userGroupMapping;
    }

    public Map<String, String> getUserCloudIdMapping() {
        return userCloudIdMapping;
    }

    public void setUserCloudIdMapping(Map<String, String> userCloudIdMapping) {
        this.userCloudIdMapping = userCloudIdMapping;
    }

    public Map<String, String> getGroupCloudIdMapping() {
        return groupCloudIdMapping;
    }

    public void setGroupCloudIdMapping(Map<String, String> groupCloudIdMapping) {
        this.groupCloudIdMapping = groupCloudIdMapping;
    }

    @Override
    public String toString( ) {
        StringBuilder sb = new StringBuilder();

        toString(sb);

        return sb.toString();
    }

    public StringBuilder toString(StringBuilder sb) {
        sb.append("RangerUserStore={")
                .append("userStoreVersion=").append(userStoreVersion).append(", ")
                .append("userStoreUpdateTime=").append(userStoreUpdateTime).append(", ");
        sb.append("users={");
        if(MapUtils.isNotEmpty(userAttrMapping)) {
            for(String user : userAttrMapping.keySet()) {
                sb.append(user);
            }
        }
        sb.append("}, ");
        sb.append("groups={");
        if(MapUtils.isNotEmpty(groupAttrMapping)) {
            for(String group : groupAttrMapping.keySet()) {
                sb.append(group);
            }
        }
        sb.append("}");
        sb.append("}");

        return sb;
    }

    private void buildMap(Set<UserInfo> users, Set<GroupInfo> groups) {
        if (CollectionUtils.isNotEmpty(users)) {
            userAttrMapping = new HashMap<>();
            userCloudIdMapping = new HashMap<>();
            for (UserInfo user : users) {
                String username = user.getName();
                Map<String, String> userAttrs = user.getOtherAttributes();
                if (MapUtils.isNotEmpty(userAttrs)) {
                    userAttrMapping.put(username, userAttrs);
                    String cloudId = userAttrs.get(CLOUD_IDENTITY_NAME);
                    if (StringUtils.isNotEmpty(cloudId)) {
                        userCloudIdMapping.put(cloudId, username);
                    }
                }
            }
        }
        
        // Always initialize groupAttrMapping to ensure it's not null for validation
        // Even if groups set is empty, we need an empty map (not null)
        groupAttrMapping = new HashMap<>();
        groupCloudIdMapping = new HashMap<>();
        
        if (CollectionUtils.isNotEmpty(groups)) {
            for (GroupInfo group : groups) {
                String groupName = group.getName();
                if (StringUtils.isEmpty(groupName)) {
                    continue;
                }
                Map<String, String> groupAttrs = group.getOtherAttributes();
                // Always add group to groupAttrMapping (with empty map if no attributes)
                // This ensures all groups are available for validation
                groupAttrMapping.put(groupName, groupAttrs != null ? groupAttrs : new HashMap<>());
                
                if (MapUtils.isNotEmpty(groupAttrs)) {
                    String cloudId = groupAttrs.get(CLOUD_IDENTITY_NAME);
                    if (StringUtils.isNotEmpty(cloudId)) {
                        groupCloudIdMapping.put(cloudId, groupName);
                    }
                }
            }
        }
    }
}
