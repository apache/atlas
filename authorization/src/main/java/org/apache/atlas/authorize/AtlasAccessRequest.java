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

import org.apache.atlas.authorize.simple.AtlasAuthorizationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.util.Date;
import java.util.Set;

public class AtlasAccessRequest {

    private static Logger LOG = LoggerFactory.getLogger(AtlasAccessRequest.class);
    private static boolean isDebugEnabled = LOG.isDebugEnabled();
    private Set<AtlasResourceTypes> resourceType = null;
    private String resource = null;
    private AtlasActionTypes action = null;
    private String user = null;
    private Set<String> userGroups = null;
    private Date accessTime = null;
    private String clientIPAddress = null;

    public AtlasAccessRequest(HttpServletRequest request, String user, Set<String> userGroups) {
        // Spring Security 4 Change => request.getServletPath() -> request.getPathInfo()
        this(AtlasAuthorizationUtils.getAtlasResourceType(request.getPathInfo()), "*", AtlasAuthorizationUtils
            .getAtlasAction(request.getMethod(),request.getPathInfo()), user, userGroups,AtlasAuthorizationUtils.getRequestIpAddress(request));
    }

    public AtlasAccessRequest(Set<AtlasResourceTypes> resourceType, String resource, AtlasActionTypes action,
        String user, Set<String> userGroups, String clientIPAddress) {
        if (isDebugEnabled) {
            LOG.debug("==> AtlasAccessRequestImpl-- Initializing AtlasAccessRequest");
        }
        setResource(resource);
        setAction(action);
        setUser(user);
        setUserGroups(userGroups);
        setResourceType(resourceType);

        // set remaining fields to default value
        setAccessTime(null);
        setClientIPAddress(clientIPAddress);
    }

    public Set<AtlasResourceTypes> getResourceTypes() {
        return resourceType;
    }

    public void setResourceType(Set<AtlasResourceTypes> resourceType) {
        this.resourceType = resourceType;
    }

    public String getResource() {
        return resource;
    }

    public void setResource(String resource) {
        this.resource = resource;
    }

    public AtlasActionTypes getAction() {
        return action;
    }

    public void setAction(AtlasActionTypes action) {
        this.action = action;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setUserGroups(Set<String> userGroups) {
        this.userGroups = userGroups;
    }

    public Set<String> getUserGroups() {
        return userGroups;
    }

    public Date getAccessTime() {
        return accessTime;
    }

    public void setAccessTime(Date accessTime) {
        this.accessTime = accessTime;
    }

    public String getClientIPAddress() {
        return clientIPAddress;
    }

    public void setClientIPAddress(String clientIPAddress) {
        this.clientIPAddress = clientIPAddress;
    }

    @Override
    public String toString() {
        return "AtlasAccessRequest [resourceType=" + resourceType + ", resource=" + resource + ", action=" + action
            + ", user=" + user + ", userGroups=" + userGroups + ", accessTime=" + accessTime + ", clientIPAddress="
            + clientIPAddress + "]";
    }

}
