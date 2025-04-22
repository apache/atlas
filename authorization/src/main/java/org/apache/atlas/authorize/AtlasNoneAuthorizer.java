/*
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

import java.util.Set;


public class AtlasNoneAuthorizer implements AtlasAuthorizer {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasNoneAuthorizer.class);

    public void init() {
        LOG.info("AtlasNoneAuthorizer.init()");
    }

    public void cleanUp() {
        LOG.info("AtlasNoneAuthorizer.cleanUp()");
    }

    public AtlasAccessResult isAccessAllowed(AtlasAdminAccessRequest request) throws AtlasAuthorizationException {
        AtlasAccessResult result = new AtlasAccessResult(true, null);
        return result;
    }

    public AtlasAccessResult isAccessAllowed(AtlasEntityAccessRequest request, boolean isAuditEnabled) throws AtlasAuthorizationException {
        AtlasAccessResult result = new AtlasAccessResult(true, null);
        return result;
    }

    public AtlasAccessResult isAccessAllowed(AtlasTypeAccessRequest request) throws AtlasAuthorizationException {
        AtlasAccessResult result = new AtlasAccessResult(true, null);
        return result;
    }

    @Override
    public AtlasAccessorResponse getAccessors(AtlasEntityAccessRequest request) {
        return null;
    }

    @Override
    public AtlasAccessorResponse getAccessors(AtlasRelationshipAccessRequest request) {
        return null;
    }

    @Override
    public AtlasAccessorResponse getAccessors(AtlasTypeAccessRequest request) {
        return null;
    }

    @Override
    public Set<String> getRolesForCurrentUser(String userName, Set<String> groups) {
        return null;
    }

    @Override
    public AtlasAccessResult isAccessAllowed(AtlasRelationshipAccessRequest request) throws AtlasAuthorizationException {
        AtlasAccessResult result = new AtlasAccessResult(true, null);
        return result;
    }

    public void scrubSearchResults(AtlasSearchResultScrubRequest request) throws AtlasAuthorizationException {

    }
}
