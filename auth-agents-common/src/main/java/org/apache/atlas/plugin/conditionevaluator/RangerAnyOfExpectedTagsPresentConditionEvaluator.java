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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.atlas.plugin.policyengine.RangerAccessRequest;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

// Policy Condition to check if resource Tags does contain any of the policy Condition Tags

public class RangerAnyOfExpectedTagsPresentConditionEvaluator extends RangerAbstractConditionEvaluator {

	private static final Log LOG = LogFactory.getLog(RangerAnyOfExpectedTagsPresentConditionEvaluator.class);

	private final Set<String> policyConditionTags = new HashSet<>();

	@Override
	public void init() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAnyOfExpectedTagsPresentConditionEvaluator.init(" + condition + ")");
		}

		super.init();

		if (condition != null ) {
			for (String value : condition.getValues()) {
				policyConditionTags.add(value.trim());
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAnyOfExpectedTagsPresentConditionEvaluator.init(" + condition + "): Tags[" + policyConditionTags + "]");
		}
	}

	@Override
	public boolean isMatched(RangerAccessRequest request) {

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAnyOfExpectedTagsPresentConditionEvaluator.isMatched(" + request + ")");
		}

		boolean matched = false;

		RangerAccessRequest			 readOnlyRequest = request.getReadOnlyCopy();
		RangerScriptExecutionContext context         = new RangerScriptExecutionContext(readOnlyRequest);
		Set<String>                  resourceTags    = context.getAllTagTypes();

		if (resourceTags != null) {
			// check if resource Tags does contain any of the policy Condition Tags
			matched = (!Collections.disjoint(resourceTags, policyConditionTags));
		}


		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAnyOfExpectedTagsPresentConditionEvaluator.isMatched(" + request+ "): " + matched);
		}

		return matched;
	}
}
