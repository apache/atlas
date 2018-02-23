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
package org.apache.atlas.authorize;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Set;

public class AtlasAccessRequest {
    private static Logger LOG = LoggerFactory.getLogger(AtlasAccessRequest.class);

    private final AtlasPrivilege action;
    private final Date           accessTime;
    private       String         user            = null;
    private       Set<String>    userGroups      = null;
    private       String         clientIPAddress = null;


    protected AtlasAccessRequest(AtlasPrivilege action) {
        this(action, null, null, new Date(), null);
    }

    protected AtlasAccessRequest(AtlasPrivilege action, String user, Set<String> userGroups) {
        this(action, user, userGroups, new Date(), null);
    }

    protected AtlasAccessRequest(AtlasPrivilege action, String user, Set<String> userGroups, Date accessTime, String clientIPAddress) {
        this.action          = action;
        this.user            = user;
        this.userGroups      = userGroups;
        this.accessTime      = accessTime;
        this.clientIPAddress = clientIPAddress;
    }

    public AtlasPrivilege getAction() {
        return action;
    }

    public Date getAccessTime() {
        return accessTime;
    }

    public String getUser() {
        return user;
    }

    public Set<String> getUserGroups() {
        return userGroups;
    }

    public void setUser(String user, Set<String> userGroups) {
        this.user       = user;
        this.userGroups = userGroups;
    }

    public String getClientIPAddress() {
        return clientIPAddress;
    }

    public void setClientIPAddress(String clientIPAddress) {
        this.clientIPAddress = clientIPAddress;
    }

    @Override
    public String toString() {
        return "AtlasAccessRequest[action=" + action + ", accessTime=" + accessTime + ", user=" + user +
                                   ", userGroups=" + userGroups + ", clientIPAddress=" + clientIPAddress + "]";
    }
}
