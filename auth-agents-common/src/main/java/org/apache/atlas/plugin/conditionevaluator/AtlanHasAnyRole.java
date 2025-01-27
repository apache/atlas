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

package org.apache.atlas.plugin.conditionevaluator;

import org.apache.atlas.plugin.policyengine.RangerAccessRequest;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class AtlanHasAnyRole extends RangerAbstractConditionEvaluator {
    private static final Log LOG = LogFactory.getLog(AtlanHasAnyRole.class);

    protected Set<String> excludedRoles = new HashSet<>();

    @Override
    public void init() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlanHasAnyRole.init(" + condition + ")");
        }

        super.init();

        if (condition != null ) {
            for (String value : condition.getValues()) {
                excludedRoles.add(value.trim());
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlanHasAnyRole.init(" + condition + ")");
        }
    }

    @Override
    public boolean isMatched(RangerAccessRequest request) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlanHasAnyRole.isMatched(" + condition + ")");
        }

        boolean ret = false;

        RangerAccessRequest	readOnlyRequest = request.getReadOnlyCopy();
        Set<String> currentRoles = ((Set<String>) readOnlyRequest.getContext().get("token:ROLES"));

        if (CollectionUtils.isNotEmpty(currentRoles)) {
            ret = !excludedRoles.stream().anyMatch(excludedRole -> currentRoles.stream().anyMatch(x -> Objects.equals(x, excludedRole)));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlanHasAnyRole.isMatched(" + condition + "): " + ret);
        }

        return ret;
    }
}
