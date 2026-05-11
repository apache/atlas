/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.web.metrics;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.audit.AtlasAuditEntry.AuditOperation;
import org.apache.atlas.repository.audit.AtlasAuditService;
import org.apache.atlas.server.common.service.EmbeddedServer;
import org.apache.atlas.server.common.service.ServiceMetricsHook;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;

/**
 * Persists {@code SERVER_START} and {@code SERVER_STATE_ACTIVE} rows for Administration → Audits.
 * <p>
 * Registered alongside {@link WebappServiceMetricsHook} as a {@link ServiceMetricsHook} so lifecycle
 * matches {@link org.apache.atlas.server.common.service.ActiveInstanceElectorService}: non-HA invokes
 * both hooks in sequence; HA emits {@code SERVER_START} at process init and {@code SERVER_STATE_ACTIVE}
 * when this instance becomes leader.
 * <p>
 * Uses the eight-argument {@link AtlasAuditService#add} overload so user and client id are explicit
 * and do not rely on {@link org.apache.atlas.RequestContext} during bootstrap.
 */
@Component
public class WebappAdminAuditHook implements ServiceMetricsHook {
    private static final Logger LOG = LoggerFactory.getLogger(WebappAdminAuditHook.class);

    private static final String ADMIN_AUDIT_USER = "atlas";

    private final AtlasAuditService auditService;

    @Inject
    public WebappAdminAuditHook(AtlasAuditService auditService) {
        this.auditService = auditService;
    }

    @Override
    public void onServerStart() {
        try {
            Date endTime = new Date();
            auditService.add(
                    ADMIN_AUDIT_USER,
                    AuditOperation.SERVER_START,
                    clientIdForAudit(),
                    EmbeddedServer.SERVER_START_TIME,
                    endTime,
                    null,
                    null,
                    0);
        } catch (AtlasBaseException e) {
            LOG.warn("Failed to write SERVER_START admin audit", e);
        }
    }

    @Override
    public void onServerActivation() {
        try {
            Date now = new Date();
            auditService.add(
                    ADMIN_AUDIT_USER,
                    AuditOperation.SERVER_STATE_ACTIVE,
                    clientIdForAudit(),
                    now,
                    now,
                    null,
                    null,
                    0);
        } catch (AtlasBaseException e) {
            LOG.warn("Failed to write SERVER_STATE_ACTIVE admin audit", e);
        }
    }

    /**
     * Aligns with {@link AtlasAuditService#add} behavior when no HTTP client is present: host:address.
     */
    private static String clientIdForAudit() {
        try {
            InetAddress local = InetAddress.getLocalHost();
            String hostName = StringUtils.defaultString(local.getHostName());
            String hostAddress = StringUtils.defaultString(local.getHostAddress());
            if (StringUtils.isNotEmpty(hostName) && StringUtils.isNotEmpty(hostAddress)) {
                return hostName + ":" + hostAddress;
            }
        } catch (UnknownHostException e) {
            LOG.debug("Could not resolve local host for audit client id", e);
        }
        return "unknown";
    }
}
