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

package org.apache.atlas.audit.destination;

import org.apache.atlas.audit.model.AuditEventBase;
import org.apache.atlas.audit.model.AuthzAuditEvent;
import org.apache.atlas.audit.provider.MiscUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.Collection;
import java.util.Properties;

public class Log4JAuditDestination extends AuditDestination {
	private static final Logger logger = LoggerFactory
			.getLogger(Log4JAuditDestination.class);

	private static Logger auditLogger = null;

	public static final String PROP_LOG4J_LOGGER = "logger";
	public static final String DEFAULT_LOGGER_PREFIX = "ranger.audit";
	private String loggerName = null;
	private static final String AUTH_AUDIT_USER = "reqUser";
	private static final String AUTH_AUDIT_ACTION = "action";
	private static final String AUTH_AUDIT_ENTITY_GUID = "entityGuid";
	private static final String AUTH_AUDIT_POLICY_ID = "policyId";
	private static final String AUTH_AUDIT_RESULT = "result";
	private static final String AUTH_AUDIT_RESOURCE= "resource";
	private static final String AUTH_AUDIT_CLIENT_IP = "cliIP";
	private static final String AUTH_AUDIT_AGENT = "agent";
	private static final String AUTH_AUDIT_ENFORCER = "enforcer";


	public Log4JAuditDestination() {
		logger.info("Log4JAuditDestination() called.");

	}

	@Override
	public void init(Properties prop, String propPrefix) {
		super.init(prop, propPrefix);
		loggerName = MiscUtil.getStringProperty(props, propPrefix + "."
				+ PROP_LOG4J_LOGGER);
		if (loggerName == null || loggerName.isEmpty()) {
			loggerName = DEFAULT_LOGGER_PREFIX + "." + getName();
			logger.info("Logger property " + propPrefix + "."
					+ PROP_LOG4J_LOGGER + " was not set. Constructing default="
					+ loggerName);
		}
		logger.info("Logger name for " + getName() + " is " + loggerName);
		auditLogger = LoggerFactory.getLogger(loggerName);
		logger.info("Done initializing logger for audit. name=" + getName()
				+ ", loggerName=" + loggerName);
	}

	
	@Override
	public void stop() {
		super.stop();
		logStatus();
	}

	@Override
	public boolean log(AuditEventBase event) {
		if (!auditLogger.isInfoEnabled()) {
			logStatusIfRequired();
			addTotalCount(1);
			return true;
		}

		if (event != null) {
			recordLogAttributes(event);
			String eventStr = MiscUtil.stringify(event);
			logJSON(eventStr);
			clearLogAttributes(event);
		}
		return true;
	}

	private void recordLogAttributes(AuditEventBase eventBase) {
		if (eventBase instanceof AuthzAuditEvent) {
			AuthzAuditEvent event = (AuthzAuditEvent) eventBase;
			MDC.put(AUTH_AUDIT_USER, event.getUser());
			MDC.put(AUTH_AUDIT_ACTION, event.getAction());
			MDC.put(AUTH_AUDIT_ENTITY_GUID, event.getEntityGuid());
			MDC.put(AUTH_AUDIT_POLICY_ID, event.getPolicyId());
			MDC.put(AUTH_AUDIT_RESOURCE, event.getResourcePath());
			MDC.put(AUTH_AUDIT_RESULT, String.valueOf(event.getAccessResult()));
			MDC.put(AUTH_AUDIT_CLIENT_IP, event.getClientIP());
			MDC.put(AUTH_AUDIT_AGENT, event.getAgentId());
			MDC.put(AUTH_AUDIT_ENFORCER, event.getAclEnforcer());
		}
	}

	private void clearLogAttributes(AuditEventBase event) {
		if (event instanceof AuthzAuditEvent) {
			MDC.remove(AUTH_AUDIT_USER);
			MDC.remove(AUTH_AUDIT_ACTION);
			MDC.remove(AUTH_AUDIT_ENTITY_GUID);
			MDC.remove(AUTH_AUDIT_POLICY_ID);
			MDC.remove(AUTH_AUDIT_RESOURCE);
			MDC.remove(AUTH_AUDIT_RESULT);
			MDC.remove(AUTH_AUDIT_CLIENT_IP);
			MDC.remove(AUTH_AUDIT_AGENT);
			MDC.remove(AUTH_AUDIT_ENFORCER);
		}
	}

	@Override
	public boolean log(Collection<AuditEventBase> events) {
		if (!auditLogger.isInfoEnabled()) {
			logStatusIfRequired();
			addTotalCount(events.size());
			return true;
		}

		for (AuditEventBase event : events) {
			log(event);
		}
		return true;
	}

	@Override
	public boolean logJSON(String event) {
		logStatusIfRequired();
		addTotalCount(1);
		if (!auditLogger.isInfoEnabled()) {
			return true;
		}

		if (event != null) {
			auditLogger.info(event);
			addSuccessCount(1);
		}
		return true;
	}

	@Override
	public boolean logJSON(Collection<String> events) {
		if (!auditLogger.isInfoEnabled()) {
			logStatusIfRequired();
			addTotalCount(events.size());
			return true;
		}

		for (String event : events) {
			logJSON(event);
		}
		return false;
	}

}
