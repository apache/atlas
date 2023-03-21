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
package org.apache.atlas.plugin.policyevaluator;

import org.apache.atlas.plugin.model.RangerPolicy;
import org.apache.atlas.plugin.model.RangerPolicy.RangerPolicyItemRowFilterInfo;
import org.apache.atlas.plugin.model.RangerPolicy.RangerRowFilterPolicyItem;
import org.apache.atlas.plugin.model.RangerServiceDef;
import org.apache.atlas.plugin.policyengine.RangerAccessResult;
import org.apache.atlas.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.atlas.plugin.policyresourcematcher.RangerPolicyResourceMatcher;


public class RangerDefaultRowFilterPolicyItemEvaluator extends RangerDefaultPolicyItemEvaluator implements RangerRowFilterPolicyItemEvaluator {
	final private RangerRowFilterPolicyItem rowFilterPolicyItem;

	public RangerDefaultRowFilterPolicyItemEvaluator(RangerServiceDef serviceDef, RangerPolicy policy, RangerRowFilterPolicyItem policyItem, int policyItemIndex, RangerPolicyEngineOptions options) {
		super(serviceDef, policy, policyItem, POLICY_ITEM_TYPE_DATAMASK, policyItemIndex, options);

		rowFilterPolicyItem = policyItem;
	}

	@Override
	public RangerPolicyItemRowFilterInfo getRowFilterInfo() {
		return rowFilterPolicyItem == null ? null : rowFilterPolicyItem.getRowFilterInfo();
	}

	@Override
	public void updateAccessResult(RangerPolicyEvaluator policyEvaluator, RangerAccessResult result, RangerPolicyResourceMatcher.MatchType matchType) {
		RangerPolicyItemRowFilterInfo rowFilterInfo = getRowFilterInfo();

		if (result.getFilterExpr() == null && rowFilterInfo != null) {
			result.setFilterExpr(rowFilterInfo.getFilterExpr());
			policyEvaluator.updateAccessResult(result, matchType, true, getComments());
		}
	}
}
