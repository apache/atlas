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

package org.apache.atlas.plugin.audit;

import org.apache.atlas.type.AtlasType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.atlas.audit.model.AuthzAuditEvent;
import org.apache.atlas.audit.provider.AuditHandler;
import org.apache.atlas.audit.provider.MiscUtil;
import org.apache.atlas.plugin.contextenricher.RangerTagForEval;
import org.apache.atlas.plugin.policyengine.*;
import org.apache.atlas.plugin.service.RangerBasePlugin;
import org.apache.atlas.plugin.util.RangerAccessRequestUtil;
import org.apache.atlas.plugin.util.RangerRESTUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;


public class RangerDefaultAuditHandler implements RangerAccessResultProcessor {
	private static final Logger LOG = LoggerFactory.getLogger(RangerDefaultAuditHandler.class);

	private static final String       CONF_AUDIT_ID_STRICT_UUID     = "xasecure.audit.auditid.strict.uuid";
	private static final boolean      DEFAULT_AUDIT_ID_STRICT_UUID  = false;


	private   final boolean         auditIdStrictUUID;
	protected final String          moduleName = null;
	private   final RangerRESTUtils restUtils      = new RangerRESTUtils();
	private         long            sequenceNumber = 0;
	private         String          UUID           = MiscUtil.generateUniqueId();
	private         AtomicInteger   counter        =  new AtomicInteger(0);



	public RangerDefaultAuditHandler() {
		auditIdStrictUUID = DEFAULT_AUDIT_ID_STRICT_UUID;
	}

	public RangerDefaultAuditHandler(Configuration config) {
		auditIdStrictUUID = config.getBoolean(CONF_AUDIT_ID_STRICT_UUID, DEFAULT_AUDIT_ID_STRICT_UUID);
	}

	@Override
	public void processResult(RangerAccessResult result) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> AtlasDefaultAccessAuditHandler.processResult(" + result + ")");
		}

		AuthzAuditEvent event = getAuthzEvents(result);

		logAuthzAudit(event);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== AtlasDefaultAccessAuditHandler.processResult(" + result + ")");
		}
	}

	@Override
	public void processResults(Collection<RangerAccessResult> results) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> AtlasDefaultAccessAuditHandler.processResults(" + results + ")");
		}

		Collection<AuthzAuditEvent> events = getAuthzEvents(results);

		if (events != null) {
			logAuthzAudits(events);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== AtlasDefaultAccessAuditHandler.processResults(" + results + ")");
		}
	}

	public AuthzAuditEvent getAuthzEvents(RangerAccessResult result) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> AtlasDefaultAccessAuditHandler.getAuthzEvents(" + result + ")");
		}

		AuthzAuditEvent ret = null;

		RangerAccessRequest request = result != null ? result.getAccessRequest() : null;

		if(request != null && result != null && result.getIsAudited()) {
			//RangerServiceDef     serviceDef   = result.getServiceDef();
			RangerAccessResource resource     = request.getResource();
			String               resourceType = resource == null ? null : resource.getLeafName();
			String               resourcePath = resource == null ? null : resource.getAsString();

			ret = createAuthzAuditEvent();

			ret.setRepositoryName(result.getServiceName());
			ret.setRepositoryType(result.getServiceType());
			ret.setResourceType(resourceType);
			ret.setResourcePath(resourcePath);
			ret.setRequestData(request.getRequestData());
			ret.setEventTime(request.getAccessTime() != null ? request.getAccessTime() : new Date());
			ret.setUser(request.getUser());
			ret.setAction(request.getAccessType());
			ret.setAccessResult((short) (result.getIsAllowed() ? 1 : 0));
			ret.setPolicyId(result.getPolicyId());
			ret.setAccessType(request.getAction());
			ret.setClientIP(request.getClientIPAddress());
			ret.setClientType(request.getClientType());
			ret.setSessionId(request.getSessionId());
			ret.setAclEnforcer(moduleName);
			Set<String> tags = getTags(request);
			if (tags != null) {
				ret.setTags(tags);
			}
			ret.setAdditionalInfo(getAdditionalInfo(request));
			ret.setClusterName(request.getClusterName());
			ret.setZoneName(result.getZoneName());
			ret.setAgentHostname(restUtils.getAgentHostname());
			ret.setPolicyVersion(result.getPolicyVersion());

			populateDefaults(ret);

			result.setAuditLogId(ret.getEventId());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== AtlasDefaultAccessAuditHandler.getAuthzEvents(" + result + "): " + ret);
		}

		return ret;
	}

	public Collection<AuthzAuditEvent> getAuthzEvents(Collection<RangerAccessResult> results) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> AtlasDefaultAccessAuditHandler.getAuthzEvents(" + results + ")");
		}

		List<AuthzAuditEvent> ret = null;

		if(results != null) {
			// TODO: optimize the number of audit logs created
			for(RangerAccessResult result : results) {
				AuthzAuditEvent event = getAuthzEvents(result);

				if(event == null) {
					continue;
				}

				if(ret == null) {
					ret = new ArrayList<>();
				}

				ret.add(event);
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== AtlasDefaultAccessAuditHandler.getAuthzEvents(" + results + "): " + ret);
		}

		return ret;
	}

	public void logAuthzAudit(AuthzAuditEvent auditEvent) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> AtlasDefaultAccessAuditHandler.logAuthzAudit(" + auditEvent + ")");
		}

		if(auditEvent != null) {
			populateDefaults(auditEvent);

			AuditHandler auditProvider = RangerBasePlugin.getAuditProvider(auditEvent.getRepositoryName());
			if (auditProvider == null || !auditProvider.log(auditEvent)) {
				MiscUtil.logErrorMessageByInterval(LOG, "fail to log audit event " + auditEvent);
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== AtlasDefaultAccessAuditHandler.logAuthzAudit(" + auditEvent + ")");
		}
	}

	private void populateDefaults(AuthzAuditEvent auditEvent) {
		if( auditEvent.getAclEnforcer() == null || auditEvent.getAclEnforcer().isEmpty()) {
			auditEvent.setAclEnforcer("ranger-acl");
		}

		if (auditEvent.getAgentHostname() == null || auditEvent.getAgentHostname().isEmpty()) {
			auditEvent.setAgentHostname(MiscUtil.getHostname());
		}

		if (auditEvent.getLogType() == null || auditEvent.getLogType().isEmpty()) {
			auditEvent.setLogType("AtlasAuthZAudit");
		}

		if (auditEvent.getEventId() == null || auditEvent.getEventId().isEmpty()) {
			auditEvent.setEventId(generateNextAuditEventId());
		}

		if (auditEvent.getAgentId() == null) {
			auditEvent.setAgentId(MiscUtil.getApplicationType());
		}

		auditEvent.setSeqNum(sequenceNumber++);
	}

	public void logAuthzAudits(Collection<AuthzAuditEvent> auditEvents) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> AtlasDefaultAccessAuditHandler.logAuthzAudits(" + auditEvents + ")");
		}

		if(auditEvents != null) {
			for(AuthzAuditEvent auditEvent : auditEvents) {
				logAuthzAudit(auditEvent);
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== AtlasDefaultAccessAuditHandler.logAuthzAudits(" + auditEvents + ")");
		}
	}

	public AuthzAuditEvent createAuthzAuditEvent() {
		return new AuthzAuditEvent();
	}

	protected final Set<String> getTags(RangerAccessRequest request) {
		Set<String>     ret  = null;
		Set<RangerTagForEval> tags = RangerAccessRequestUtil.getRequestTagsFromContext(request.getContext());

		if (CollectionUtils.isNotEmpty(tags)) {
			ret = new HashSet<>();

			for (RangerTagForEval tag : tags) {
				ret.add(writeObjectAsString(tag));
			}
		}

		return ret;
	}

	public 	String getAdditionalInfo(RangerAccessRequest request) {
		if (StringUtils.isBlank(request.getRemoteIPAddress()) && CollectionUtils.isEmpty(request.getForwardedAddresses())) {
			return null;
		}
		StringBuilder sb = new StringBuilder();
		sb.append("{\"remote-ip-address\":").append(request.getRemoteIPAddress())
				.append(", \"forwarded-ip-addresses\":[").append(StringUtils.join(request.getForwardedAddresses(), ", ")).append("]");

		return sb.toString();
	}

	private String generateNextAuditEventId() {
		final String ret;

		if (auditIdStrictUUID) {
			ret = MiscUtil.generateGuid();
		} else {
			int nextId = counter.getAndIncrement();

			if (nextId == Integer.MAX_VALUE) {
				// reset UUID and counter
				UUID    = MiscUtil.generateUniqueId();
				counter = new AtomicInteger(0);
			}

			ret = UUID + "-" + Integer.toString(nextId);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("generateNextAuditEventId(): " + ret);
		}

		return ret;
	}

	private String writeObjectAsString(Serializable obj) {
		String jsonStr = StringUtils.EMPTY;
		try {
			jsonStr = AtlasType.toJson(obj);
		} catch (Exception e) {
			LOG.error("Cannot create JSON string for object:[" + obj + "]", e);
		}
		return jsonStr;
	}
}
