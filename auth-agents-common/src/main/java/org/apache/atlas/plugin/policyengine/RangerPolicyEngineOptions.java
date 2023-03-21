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
package org.apache.atlas.plugin.policyengine;

import org.apache.hadoop.conf.Configuration;
import org.apache.atlas.plugin.model.validation.RangerServiceDefHelper;
import org.apache.atlas.plugin.policyevaluator.RangerPolicyEvaluator;

public class RangerPolicyEngineOptions {
	public String evaluatorType = RangerPolicyEvaluator.EVALUATOR_TYPE_AUTO;

	public boolean disableContextEnrichers = false;
	public boolean disableCustomConditions = false;
	public boolean disableTagPolicyEvaluation = false;
	public boolean disableTrieLookupPrefilter = false;
	public boolean disablePolicyRefresher = false;
	public boolean disableTagRetriever = false;
	public boolean cacheAuditResults = true;
	public boolean evaluateDelegateAdminOnly = false;
	public boolean enableTagEnricherWithLocalRefresher = false;
	public boolean disableAccessEvaluationWithPolicyACLSummary = true;
	public boolean optimizeTrieForRetrieval = false;
	public boolean disableRoleResolution = true;

	private RangerServiceDefHelper serviceDefHelper;

	public RangerPolicyEngineOptions() {}

	public RangerPolicyEngineOptions(final RangerPolicyEngineOptions other) {
		this.disableContextEnrichers = other.disableContextEnrichers;
		this.disableCustomConditions = other.disableCustomConditions;
		this.disableTagPolicyEvaluation = other.disableTagPolicyEvaluation;
		this.disableTrieLookupPrefilter = other.disableTrieLookupPrefilter;
		this.disablePolicyRefresher = other.disablePolicyRefresher;
		this.disableTagRetriever = other.disableTagRetriever;
		this.cacheAuditResults = other.cacheAuditResults;
		this.evaluateDelegateAdminOnly = other.evaluateDelegateAdminOnly;
		this.enableTagEnricherWithLocalRefresher = other.enableTagEnricherWithLocalRefresher;
		this.disableAccessEvaluationWithPolicyACLSummary = other.disableAccessEvaluationWithPolicyACLSummary;
		this.optimizeTrieForRetrieval = other.optimizeTrieForRetrieval;
		this.disableRoleResolution = other.disableRoleResolution;
		this.serviceDefHelper = null;
	}

	public void configureForPlugin(Configuration conf, String propertyPrefix) {
		disableContextEnrichers = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.context.enrichers", false);
		disableCustomConditions = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.custom.conditions", false);
		disableTagPolicyEvaluation = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.tagpolicy.evaluation", false);
		disableTrieLookupPrefilter = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.trie.lookup.prefilter", false);
		disablePolicyRefresher = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.policy.refresher", false);
		disableTagRetriever = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.tag.retriever", false);

		cacheAuditResults = conf.getBoolean(propertyPrefix + ".policyengine.option.cache.audit.results", true);

		if (!disableTrieLookupPrefilter) {
			cacheAuditResults = false;
		}
		evaluateDelegateAdminOnly = false;
		enableTagEnricherWithLocalRefresher = false;
		disableAccessEvaluationWithPolicyACLSummary = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.access.evaluation.with.policy.acl.summary", true);
		optimizeTrieForRetrieval = conf.getBoolean(propertyPrefix + ".policyengine.option.optimize.trie.for.retrieval", false);
		disableRoleResolution = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.role.resolution", true);

	}

	public void configureDefaultRangerAdmin(Configuration conf, String propertyPrefix) {
		disableContextEnrichers = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.context.enrichers", true);
		disableCustomConditions = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.custom.conditions", true);
		disableTagPolicyEvaluation = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.tagpolicy.evaluation", true);
		disableTrieLookupPrefilter = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.trie.lookup.prefilter", false);
		disablePolicyRefresher = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.policy.refresher", true);
		disableTagRetriever = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.tag.retriever", true);

		cacheAuditResults = false;
		evaluateDelegateAdminOnly = false;
		enableTagEnricherWithLocalRefresher = false;
		disableAccessEvaluationWithPolicyACLSummary = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.access.evaluation.with.policy.acl.summary", true);
		optimizeTrieForRetrieval = conf.getBoolean(propertyPrefix + ".policyengine.option.optimize.trie.for.retrieval", false);
		disableRoleResolution = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.role.resolution", true);

	}

	public void configureDelegateAdmin(Configuration conf, String propertyPrefix) {
		disableContextEnrichers = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.context.enrichers", true);
		disableCustomConditions = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.custom.conditions", true);
		disableTagPolicyEvaluation = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.tagpolicy.evaluation", true);
		disableTrieLookupPrefilter = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.trie.lookup.prefilter", false);
		disablePolicyRefresher = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.policy.refresher", true);
		disableTagRetriever = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.tag.retriever", true);
		optimizeTrieForRetrieval = conf.getBoolean(propertyPrefix + ".policyengine.option.optimize.trie.for.retrieval", false);


		cacheAuditResults = false;
		evaluateDelegateAdminOnly = true;
		enableTagEnricherWithLocalRefresher = false;

	}

	public void configureRangerAdminForPolicySearch(Configuration conf, String propertyPrefix) {
		disableContextEnrichers = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.context.enrichers", true);
		disableCustomConditions = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.custom.conditions", true);
		disableTagPolicyEvaluation = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.tagpolicy.evaluation", false);
		disableTrieLookupPrefilter = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.trie.lookup.prefilter", false);
		disablePolicyRefresher = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.policy.refresher", true);
		disableTagRetriever = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.tag.retriever", false);
		optimizeTrieForRetrieval = conf.getBoolean(propertyPrefix + ".policyengine.option.optimize.trie.for.retrieval", false);


		cacheAuditResults = false;
		evaluateDelegateAdminOnly = false;
		enableTagEnricherWithLocalRefresher = true;
	}

	public RangerServiceDefHelper getServiceDefHelper() {
		 return serviceDefHelper;
	}

	void setServiceDefHelper(RangerServiceDefHelper serviceDefHelper) {
		this.serviceDefHelper = serviceDefHelper;
	}

	/*
	* There is no need to implement these, as the options are predefined in a component ServiceREST and hence
	* guaranteed to be unique objects. That implies that the default equals and hashCode should suffice.
	*/

	@Override
	public boolean equals(Object other) {
		boolean ret = false;
		if (other instanceof RangerPolicyEngineOptions) {
			RangerPolicyEngineOptions that = (RangerPolicyEngineOptions) other;
			ret = this.disableContextEnrichers == that.disableContextEnrichers
					&& this.disableCustomConditions == that.disableCustomConditions
					&& this.disableTagPolicyEvaluation == that.disableTagPolicyEvaluation
					&& this.disableTrieLookupPrefilter == that.disableTrieLookupPrefilter
					&& this.disablePolicyRefresher == that.disablePolicyRefresher
					&& this.disableTagRetriever == that.disableTagRetriever
					&& this.cacheAuditResults == that.cacheAuditResults
					&& this.evaluateDelegateAdminOnly == that.evaluateDelegateAdminOnly
					&& this.enableTagEnricherWithLocalRefresher == that.enableTagEnricherWithLocalRefresher
					&& this.optimizeTrieForRetrieval == that.optimizeTrieForRetrieval
					&& this.disableRoleResolution == that.disableRoleResolution;
		}
		return ret;
	}

	@Override
	public int hashCode() {
		int ret = 0;
		ret += disableContextEnrichers ? 1 : 0;
		ret *= 2;
		ret += disableCustomConditions ? 1 : 0;
		ret *= 2;
		ret += disableTagPolicyEvaluation ? 1 : 0;
		ret *= 2;
		ret += disableTrieLookupPrefilter ? 1 : 0;
		ret *= 2;
		ret += disablePolicyRefresher ? 1 : 0;
		ret *= 2;
		ret += disableTagRetriever ? 1 : 0;
		ret *= 2;
		ret += cacheAuditResults ? 1 : 0;
		ret *= 2;
		ret += evaluateDelegateAdminOnly ? 1 : 0;
		ret *= 2;
		ret += enableTagEnricherWithLocalRefresher ? 1 : 0;
		ret *= 2;
		ret += optimizeTrieForRetrieval ? 1 : 0;
		ret *= 2;
		ret += disableRoleResolution ? 1 : 0;
		ret *= 2;		return ret;
	}

	@Override
	public String toString() {
		return "PolicyEngineOptions: {" +
				" evaluatorType: " + evaluatorType +
				", evaluateDelegateAdminOnly: " + evaluateDelegateAdminOnly +
				", disableContextEnrichers: " + disableContextEnrichers +
				", disableCustomConditions: " + disableContextEnrichers +
				", disableTagPolicyEvaluation: " + disableTagPolicyEvaluation +
				", disablePolicyRefresher: " + disablePolicyRefresher +
				", disableTagRetriever: " + disableTagRetriever +
				", enableTagEnricherWithLocalRefresher: " + enableTagEnricherWithLocalRefresher +
				", disableTrieLookupPrefilter: " + disableTrieLookupPrefilter +
				", optimizeTrieForRetrieval: " + optimizeTrieForRetrieval +
				", cacheAuditResult: " + cacheAuditResults +
				", disableRoleResolution: " + disableRoleResolution +
				" }";

	}
}
