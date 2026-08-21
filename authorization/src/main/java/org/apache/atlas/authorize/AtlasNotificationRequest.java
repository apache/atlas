/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.authorize;

import java.util.Set;

public class AtlasNotificationRequest extends AtlasAccessRequest {
    private final String topicName;

    public AtlasNotificationRequest(AtlasPrivilege action, String topicName) {
        super(action);

        this.topicName = topicName;
    }

    public AtlasNotificationRequest(AtlasPrivilege action, String topicName, String userName, Set<String> userGroups) {
        super(action, userName, userGroups);

        this.topicName = topicName;
    }

    public String getTopicName() {
        return topicName;
    }

    /**
     * Resource type for Ranger and policy engines ({@code notification-topic}).
     */
    public String getResourceType() {
        return AtlasAuthorizeConstants.NOTIFICATION_TOPIC_RESOURCE_TYPE;
    }

    @Override
    public String toString() {
        return "AtlasNotificationRequest[resourceType=" + getResourceType() + ", action=" + getAction() + ", topicName=" + topicName + ", accessTime=" + getAccessTime() + ", user=" + getUser() +
                ", userGroups=" + getUserGroups() + ", clientIPAddress=" + getClientIPAddress() +
                ", forwardedAddresses=" + getForwardedAddresses() + ", remoteIPAddress=" + getRemoteIPAddress() + "]";
    }
}
