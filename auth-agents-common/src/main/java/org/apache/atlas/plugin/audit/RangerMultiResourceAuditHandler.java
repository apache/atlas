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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.atlas.audit.model.AuthzAuditEvent;

import java.util.ArrayList;
import java.util.Collection;

public class RangerMultiResourceAuditHandler extends RangerDefaultAuditHandler {
	private static final Log LOG = LogFactory.getLog(RangerMultiResourceAuditHandler.class);

	Collection<AuthzAuditEvent> auditEvents = new ArrayList<>();

	public RangerMultiResourceAuditHandler() {
	}


	@Override
	public void logAuthzAudit(AuthzAuditEvent auditEvent) {
		auditEvents.add(auditEvent);
	}

	@Override
	public void logAuthzAudits(Collection<AuthzAuditEvent> auditEvents) {
		this.auditEvents.addAll(auditEvents);
	}

	public void flushAudit() {
		try {
			boolean deniedExists = false;
			// First iterate to see if there are any denied
			for (AuthzAuditEvent auditEvent : auditEvents) {
				if (auditEvent.getAccessResult() == 0) {
					deniedExists = true;
					break;
				}
			}

			for (AuthzAuditEvent auditEvent : auditEvents) {
				if (deniedExists && auditEvent.getAccessResult() != 0) {
					continue;
				}

				super.logAuthzAudit(auditEvent);
			}
		} catch (Throwable t) {
			LOG.error("Error occured while writing audit log... ", t);
		} finally {
			// reset auditEvents once audits are logged
			auditEvents = new ArrayList<>();
		}
	}
}
