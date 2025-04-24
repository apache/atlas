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

import java.util.*;


public class AtlanExcludeResourceByType extends RangerAbstractConditionEvaluator {
    private static final Log LOG = LogFactory.getLog(AtlanExcludeResourceByType.class);
    protected Set<String> excludeEntityTypes = new HashSet<>();
    public static final String RESOURCE_ENTITY_TYPE                   = "entity-type";
    public static final String RESOURCE_END_ONE_ENTITY_TYPE           = "end-one-entity-type";
    public static final String RESOURCE_END_TWO_ENTITY_TYPE           =  "end-two-entity-type";


    @Override
    public void init() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlanExcludeResourceByType.init(" + condition + ")");
        }

        super.init();

        if (condition != null ) {
            for (String value : condition.getValues()) {
                excludeEntityTypes.add(value.trim());
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlanExcludeResourceByType.init(" + condition + ")");
        }
    }

    @Override
    public boolean isMatched(RangerAccessRequest request) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlanExcludeResourceByType.isMatched(" + condition + ")");
        }

        boolean ret = true;
        RangerAccessRequest readOnlyRequest = request.getReadOnlyCopy();

        Set<Object> entityTypes = new HashSet<>();
        Object entityType = readOnlyRequest.getResource().getValue(RESOURCE_ENTITY_TYPE);
        Object endOneEntityType = readOnlyRequest.getResource().getValue(RESOURCE_END_ONE_ENTITY_TYPE);
        Object endTwoEntityType = readOnlyRequest.getResource().getValue(RESOURCE_END_TWO_ENTITY_TYPE);

        if (entityType != null) {
            entityTypes.addAll(convertToSet(entityType));
        } else if (endOneEntityType != null) {
            entityTypes.addAll(convertToSet(endOneEntityType));
        } else if (endTwoEntityType != null) {
            entityTypes.addAll(convertToSet(endTwoEntityType));
        }

        if (!entityTypes.isEmpty()) {
            ret = excludeEntityTypes.stream().noneMatch(entityTypes::contains);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlanExcludeResourceByType.isMatched(" + condition + "): " + ret);
        }
        return ret;
    }

    private Set<Object> convertToSet(Object obj) {
        if (obj instanceof Object[]) {
            return new HashSet<>(Arrays.asList((Object[]) obj));
        } else if (obj instanceof Collection) {
            return new HashSet<>((Collection<?>) obj);
        }
        return Collections.emptySet();
    }

}
