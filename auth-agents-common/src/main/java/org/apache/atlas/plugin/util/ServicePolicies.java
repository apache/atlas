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

package org.apache.atlas.plugin.util;


import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.atlas.plugin.model.RangerPolicy;
import org.apache.atlas.plugin.model.RangerPolicyDelta;
import org.apache.atlas.plugin.model.RangerServiceDef;
import org.apache.atlas.plugin.policyengine.RangerPolicyEngine;
import org.apache.atlas.plugin.policyengine.RangerPolicyEngineImpl;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.*;

@JsonInclude(JsonInclude.Include.NON_NULL)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ServicePolicies implements java.io.Serializable {
	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(ServicePolicies.class);

	private String             serviceName;
	private String             serviceId;
	private Long               policyVersion;
	private Date               policyUpdateTime;
	private List<RangerPolicy> policies;
	private RangerServiceDef   serviceDef;
	private String             auditMode = RangerPolicyEngine.AUDIT_DEFAULT;
	private TagPolicies        tagPolicies;
	private Map<String, SecurityZoneInfo> securityZones;
	private List<RangerPolicyDelta> policyDeltas;

	private Map<String, RangerPolicyDelta> deleteDeltas;
	private Map<String, String> serviceConfig;

	/**
	 * @return the serviceName
	 */
	public String getServiceName() {
		return serviceName;
	}
	/**
	 * @param serviceName the serviceName to set
	 */
	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}
	/**
	 * @return the serviceId
	 */
	public String getServiceId() {
		return serviceId;
	}
	/**
	 * @param serviceId the serviceId to set
	 */
	public void setServiceId(String serviceId) {
		this.serviceId = serviceId;
	}
	/**
	 * @return the policyVersion
	 */
	public Long getPolicyVersion() {
		return policyVersion;
	}
	/**
	 * @param policyVersion the policyVersion to set
	 */
	public void setPolicyVersion(Long policyVersion) {
		this.policyVersion = policyVersion;
	}
	/**
	 * @return the policyUpdateTime
	 */
	public Date getPolicyUpdateTime() {
		return policyUpdateTime;
	}
	/**
	 * @param policyUpdateTime the policyUpdateTime to set
	 */
	public void setPolicyUpdateTime(Date policyUpdateTime) {
		this.policyUpdateTime = policyUpdateTime;
	}

	public Map<String, String> getServiceConfig() {
		return serviceConfig;
	}
	public void setServiceConfig(Map<String, String> serviceConfig) {
		this.serviceConfig = serviceConfig;
	}

	/**
	 * @return the policies
	 */
	public List<RangerPolicy> getPolicies() {
		return policies;
	}
	/**
	 * @param policies the policies to set
	 */
	public void setPolicies(List<RangerPolicy> policies) {
		this.policies = policies;
	}
	/**
	 * @return the serviceDef
	 */
	public RangerServiceDef getServiceDef() {
		return serviceDef;
	}
	/**
	 * @param serviceDef the serviceDef to set
	 */
	public void setServiceDef(RangerServiceDef serviceDef) {
		this.serviceDef = serviceDef;
	}

	public String getAuditMode() {
		return auditMode;
	}

	public void setAuditMode(String auditMode) {
		this.auditMode = auditMode;
	}
	/**
	 * @return the tagPolicies
	 */
	public TagPolicies getTagPolicies() {
		return tagPolicies;
	}
	/**
	 * @param tagPolicies the tagPolicies to set
	 */
	public void setTagPolicies(TagPolicies tagPolicies) {
		this.tagPolicies = tagPolicies;
	}

	public Map<String, SecurityZoneInfo> getSecurityZones() { return securityZones; }

	public void setSecurityZones(Map<String, SecurityZoneInfo> securityZones) {
		this.securityZones = securityZones;
	}

	@Override
	public String toString() {
		return "serviceName=" + serviceName + ", "
				+ "serviceId=" + serviceId + ", "
			 	+ "policyVersion=" + policyVersion + ", "
			 	+ "policyUpdateTime=" + policyUpdateTime + ", "
			 	+ "policies=" + policies + ", "
			 	+ "tagPolicies=" + tagPolicies + ", "
			 	+ "policyDeltas=" + policyDeltas + ", "
			 	+ "serviceDef=" + serviceDef + ", "
			 	+ "auditMode=" + auditMode + ", "
				+ "securityZones=" + securityZones
				;
	}

	public List<RangerPolicyDelta> getPolicyDeltas() { return this.policyDeltas; }

	public void setPolicyDeltas(List<RangerPolicyDelta> policyDeltas) { this.policyDeltas = policyDeltas; }

	public Map<String, RangerPolicyDelta> getDeleteDeltas() {
		return deleteDeltas;
	}

	public void setDeleteDeltas(Map<String, RangerPolicyDelta> deleteDeltas) {
		this.deleteDeltas = deleteDeltas;
	}

	@JsonInclude(JsonInclude.Include.NON_NULL)
	@XmlRootElement
	@XmlAccessorType(XmlAccessType.FIELD)
	public static class TagPolicies implements java.io.Serializable {
		private static final long serialVersionUID = 1L;

		private String             serviceName;
		private String             serviceId;
		private Long               policyVersion;
		private Date               policyUpdateTime;
		private List<RangerPolicy> policies;
		private RangerServiceDef   serviceDef;
		private String             auditMode = RangerPolicyEngine.AUDIT_DEFAULT;
		private Map<String, String> serviceConfig;

		/**
		 * @return the serviceName
		 */
		public String getServiceName() {
			return serviceName;
		}
		/**
		 * @param serviceName the serviceName to set
		 */
		public void setServiceName(String serviceName) {
			this.serviceName = serviceName;
		}
		/**
		 * @return the serviceId
		 */
		public String getServiceId() {
			return serviceId;
		}
		/**
		 * @param serviceId the serviceId to set
		 */
		public void setServiceId(String serviceId) {
			this.serviceId = serviceId;
		}
		/**
		 * @return the policyVersion
		 */
		public Long getPolicyVersion() {
			return policyVersion;
		}
		/**
		 * @param policyVersion the policyVersion to set
		 */
		public void setPolicyVersion(Long policyVersion) {
			this.policyVersion = policyVersion;
		}
		/**
		 * @return the policyUpdateTime
		 */
		public Date getPolicyUpdateTime() {
			return policyUpdateTime;
		}
		/**
		 * @param policyUpdateTime the policyUpdateTime to set
		 */
		public void setPolicyUpdateTime(Date policyUpdateTime) {
			this.policyUpdateTime = policyUpdateTime;
		}
		/**
		 * @return the policies
		 */
		public List<RangerPolicy> getPolicies() {
			return policies;
		}
		/**
		 * @param policies the policies to set
		 */
		public void setPolicies(List<RangerPolicy> policies) {
			this.policies = policies;
		}
		/**
		 * @return the serviceDef
		 */
		public RangerServiceDef getServiceDef() {
			return serviceDef;
		}
		/**
		 * @param serviceDef the serviceDef to set
		 */
		public void setServiceDef(RangerServiceDef serviceDef) {
			this.serviceDef = serviceDef;
		}

		public String getAuditMode() {
			return auditMode;
		}

		public void setAuditMode(String auditMode) {
			this.auditMode = auditMode;
		}

		public Map<String, String> getServiceConfig() {
			return serviceConfig;
		}

		public void setServiceConfig(Map<String, String> serviceConfig) {
			this.serviceConfig = serviceConfig;
		}

		@Override
		public String toString() {
			return "serviceName=" + serviceName + ", "
					+ "serviceId=" + serviceId + ", "
					+ "policyVersion=" + policyVersion + ", "
					+ "policyUpdateTime=" + policyUpdateTime + ", "
					+ "policies=" + policies + ", "
					+ "serviceDef=" + serviceDef + ", "
					+ "auditMode=" + auditMode
					+ "serviceConfig=" + serviceConfig
					;
		}
	}

	@JsonInclude(JsonInclude.Include.NON_NULL)
	@XmlRootElement
	@XmlAccessorType(XmlAccessType.FIELD)
	public static class SecurityZoneInfo implements java.io.Serializable {
		private static final long serialVersionUID = 1L;
		private String                          zoneName;
		private List<HashMap<String, List<String>>> resources;
		private List<RangerPolicy>              policies;
		private List<RangerPolicyDelta>         policyDeltas;
		private Boolean                         containsAssociatedTagService;

		public String getZoneName() {
			return zoneName;
		}

		public List<HashMap<String, List<String>>> getResources() {
			return resources;
		}

		public List<RangerPolicy> getPolicies() {
			return policies;
		}

		public List<RangerPolicyDelta> getPolicyDeltas() { return policyDeltas; }

		public Boolean getContainsAssociatedTagService() { return containsAssociatedTagService; }

		public void setZoneName(String zoneName) {
			this.zoneName = zoneName;
		}

		public void setResources(List<HashMap<String, List<String>>> resources) {
			this.resources = resources;
		}

		public void setPolicies(List<RangerPolicy> policies) {
			this.policies = policies;
		}

		public void setPolicyDeltas(List<RangerPolicyDelta> policyDeltas) { this.policyDeltas = policyDeltas; }

		public void setContainsAssociatedTagService(Boolean containsAssociatedTagService) { this.containsAssociatedTagService = containsAssociatedTagService; }

		@Override
		public String toString() {
			return "zoneName=" + zoneName + ", "
					+ "resources=" + resources + ", "
					+ "policies=" + policies + ", "
					+ "policyDeltas=" + policyDeltas + ", "
					+ "containsAssociatedTagService=" + containsAssociatedTagService
					;
		}
	}

	static public ServicePolicies copyHeader(ServicePolicies source) {
		ServicePolicies ret = new ServicePolicies();

		ret.setServiceName(source.getServiceName());
		ret.setServiceId(source.getServiceId());
		ret.setPolicyVersion(source.getPolicyVersion());
		ret.setAuditMode(source.getAuditMode());
		ret.setServiceDef(source.getServiceDef());
		ret.setPolicyUpdateTime(source.getPolicyUpdateTime());
		ret.setSecurityZones(source.getSecurityZones());
		ret.setPolicies(Collections.emptyList());
		ret.setPolicyDeltas(null);
		if (source.getTagPolicies() != null) {
			TagPolicies tagPolicies = copyHeader(source.getTagPolicies(), source.getServiceDef().getName());
			ret.setTagPolicies(tagPolicies);
		}

		return ret;
	}

	static public TagPolicies copyHeader(TagPolicies source, String componentServiceName) {
		TagPolicies ret = new TagPolicies();

		ret.setServiceName(source.getServiceName());
		ret.setServiceId(source.getServiceId());
		ret.setPolicyVersion(source.getPolicyVersion());
		ret.setAuditMode(source.getAuditMode());
		ret.setServiceDef(ServiceDefUtil.normalizeAccessTypeDefs(source.getServiceDef(), componentServiceName));
		ret.setPolicyUpdateTime(source.getPolicyUpdateTime());
		ret.setPolicies(Collections.emptyList());

		return ret;
	}

	public static ServicePolicies applyDelta(final ServicePolicies servicePolicies, RangerPolicyEngineImpl policyEngine) {
		ServicePolicies ret = copyHeader(servicePolicies);

		List<RangerPolicy> oldResourcePolicies = policyEngine.getResourcePolicies();
		List<RangerPolicy> oldTagPolicies      = policyEngine.getTagPolicies();

		List<RangerPolicy> resourcePoliciesAfterDelete =
				RangerPolicyDeltaUtil.deletePoliciesByDelta(oldResourcePolicies, servicePolicies.getDeleteDeltas());
		List<RangerPolicy> newResourcePolicies =
				RangerPolicyDeltaUtil.applyDeltas(resourcePoliciesAfterDelete, servicePolicies.getPolicyDeltas(), servicePolicies.getServiceDef().getName());

		ret.setPolicies(newResourcePolicies);

		List<RangerPolicy> newTagPolicies;
		if (servicePolicies.getTagPolicies() != null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("applyingDeltas for tag policies");
			}
			List<RangerPolicy> tagPoliciesAfterDelete =
					RangerPolicyDeltaUtil.deletePoliciesByDelta(oldTagPolicies, servicePolicies.getDeleteDeltas());
			newTagPolicies = RangerPolicyDeltaUtil.applyDeltas(tagPoliciesAfterDelete, servicePolicies.getPolicyDeltas(), servicePolicies.getTagPolicies().getServiceDef().getName());
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("No need to apply deltas for tag policies");
			}
			newTagPolicies = oldTagPolicies;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("New tag policies: " + newTagPolicies);
		}

		if (ret.getTagPolicies() != null) {
			ret.getTagPolicies().setPolicies(newTagPolicies);
		}

		return ret;
	}
}
