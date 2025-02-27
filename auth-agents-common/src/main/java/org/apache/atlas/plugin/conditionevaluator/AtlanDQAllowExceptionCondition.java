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


public class AtlanDQAllowExceptionCondition extends RangerAbstractConditionEvaluator {
    private static final Log LOG = LogFactory.getLog(AtlanHasAnyRole.class);
    protected Set<String> excludedActions = new HashSet<>();
    public static final String RESOURCE_ENTITY_TYPE                   = "entity-type";
    public static final String RESOURCE_ENTITY_CLASSIFICATION         = "entity-classification";
    public static final String RESOURCE_CLASSIFICATION                = "classification";
    public static final String RESOURCE_ENTITY_ID                     = "entity";
    public static final String RESOURCE_ENTITY_LABEL                  = "entity-label";
    public static final String RESOURCE_ENTITY_BUSINESS_METADATA      = "entity-business-metadata";
    public static final String RESOURCE_ENTITY_OWNER                  = "owner";
    public static final String RESOURCE_RELATIONSHIP_TYPE             = "relationship-type";
    public static final String RESOURCE_END_ONE_ENTITY_TYPE           = "end-one-entity-type";
    public static final String RESOURCE_END_ONE_ENTITY_CLASSIFICATION = "end-one-entity-classification";
    public static final String RESOURCE_END_ONE_ENTITY_ID             = "end-one-entity";
    public static final String RESOURCE_END_TWO_ENTITY_TYPE           =  "end-two-entity-type";


    @Override
    public void init() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlanDQAllowExceptionCondition.init(" + condition + ")");
        }

        super.init();

        if (condition != null ) {
            for (String value : condition.getValues()) {
                excludedActions.add(value.trim());
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlanDQAllowExceptionCondition.init(" + condition + ")");
        }
    }

    @Override
    public boolean isMatched(RangerAccessRequest request) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlanDQAllowExceptionCondition.isMatched(" + condition + ")");
        }

        boolean ret = true;
        RangerAccessRequest	readOnlyRequest = request.getReadOnlyCopy();

        Object[] entityTypeObjects = {
                readOnlyRequest.getResource().getValue(RESOURCE_ENTITY_TYPE),
                readOnlyRequest.getResource().getValue(RESOURCE_END_ONE_ENTITY_TYPE),
                readOnlyRequest.getResource().getValue(RESOURCE_END_TWO_ENTITY_TYPE)
        };

        List<?> entityTypes = Arrays.stream(entityTypeObjects)
                .filter(Objects::nonNull)
                .map(this::convertToList)
                .findFirst()
                .orElseGet(ArrayList::new);

        if (entityTypes.isEmpty() || !entityTypes.contains("alpha_DQRule")) {
            return ret;
        }
        else {
            return !excludedActions.contains(readOnlyRequest.getAction());
        }

    }

    private List<?> convertToList(Object obj) {
        if (obj instanceof Object[]) {
            return Arrays.asList((Object[]) obj);
        } else if (obj instanceof Collection) {
            return new ArrayList<>((Collection<?>) obj);
        }
        return Collections.emptyList();
    }

}
