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


public class AtlasAccessResult {
    private static Logger LOG = LoggerFactory.getLogger(AtlasAccessResult.class);

    private boolean isAllowed = false;
    private String  policyId  = "-1";
    private int  policyPriority = -1;
    private boolean explicitDeny = false;
    private String enforcer = "abac_auth";

    public AtlasAccessResult() {

    }

    public AtlasAccessResult(boolean isAllowed) {
        this.isAllowed = isAllowed;
    }

    public AtlasAccessResult(boolean isAllowed, String policyId) {
        this(isAllowed);
        this.policyId = policyId;

        this.explicitDeny = !isAllowed && !"-1".equals(policyId);
    }

    public AtlasAccessResult(boolean isAllowed, String policyId, int policyPriority) {
        this(isAllowed, policyId);
        this.policyPriority = policyPriority;
    }

    public boolean isExplicitDeny() {
        return explicitDeny;
    }

    public boolean isAllowed() {
        return isAllowed;
    }

    public int getPolicyPriority() {
        return policyPriority;
    }

    public void setAllowed(boolean allowed) {
        isAllowed = allowed;
    }

    public String getPolicyId() {
        return policyId;
    }

    public void setPolicyId(String policyId) {
        this.policyId = policyId;
    }

    public void setPolicyPriority(int policyPriority) {
        this.policyPriority = policyPriority;
    }

    public void setEnforcer(String enforcer) { this.enforcer = enforcer; }

    public String getEnforcer() { return enforcer; }
}
