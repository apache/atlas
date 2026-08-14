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
package org.apache.atlas.server.common.service;

/**
 * ZooKeeper and HA connection parameters in a form shared code can use without importing
 * webapp-specific or rest-notification-specific configuration classes.
 */
public class HighAvailabilityProperties {
    private final String connectString;
    private final String zkRoot;
    private final int    retriesSleepTimeMillis;
    private final int    numRetries;
    private final int    sessionTimeout;
    private final String acl;
    private final String auth;

    public HighAvailabilityProperties(String connectString, String zkRoot, int retriesSleepTimeMillis, int numRetries,
            int sessionTimeout, String acl, String auth) {
        this.connectString          = connectString;
        this.zkRoot                 = zkRoot;
        this.retriesSleepTimeMillis = retriesSleepTimeMillis;
        this.numRetries             = numRetries;
        this.sessionTimeout         = sessionTimeout;
        this.acl                    = acl;
        this.auth                   = auth;
    }

    public String getConnectString() {
        return connectString;
    }

    public String getZkRoot() {
        return zkRoot;
    }

    public int getRetriesSleepTimeMillis() {
        return retriesSleepTimeMillis;
    }

    public int getNumRetries() {
        return numRetries;
    }

    public int getSessionTimeout() {
        return sessionTimeout;
    }

    public String getAcl() {
        return acl;
    }

    public String getAuth() {
        return auth;
    }

    public boolean hasAcl() {
        return acl != null;
    }

    public boolean hasAuth() {
        return auth != null;
    }
}
