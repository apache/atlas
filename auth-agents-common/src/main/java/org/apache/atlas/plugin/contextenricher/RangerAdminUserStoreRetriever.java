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

package org.apache.atlas.plugin.contextenricher;

import org.apache.atlas.authz.admin.client.AtlasAuthAdminClient;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.atlas.admin.client.RangerAdminClient;
import org.apache.atlas.authorization.hadoop.config.RangerPluginConfig;
import org.apache.atlas.plugin.policyengine.RangerPluginContext;
import org.apache.atlas.plugin.util.RangerUserStore;

import java.nio.channels.ClosedByInterruptException;
import java.util.Map;

public class RangerAdminUserStoreRetriever extends RangerUserStoreRetriever {
    private static final Log LOG = LogFactory.getLog(RangerAdminUserStoreRetriever.class);

    private AtlasAuthAdminClient atlasAuthAdminClient;

    @Override
    public void init(Map<String, String> options) {

        if (StringUtils.isNotBlank(serviceName) && serviceDef != null && StringUtils.isNotBlank(appId)) {
            RangerPluginConfig pluginConfig = super.pluginConfig;

            if (pluginConfig == null) {
                pluginConfig = new RangerPluginConfig(serviceDef.getName(), serviceName, appId, null, null, null);
            }

            RangerPluginContext pluginContext = getPluginContext();
            AtlasAuthAdminClient   adminClient = pluginContext.getAtlasAuthAdminClient();
            this.atlasAuthAdminClient          = (adminClient != null) ? adminClient : pluginContext.createAtlasAuthAdminClient(pluginConfig);

        } else {
            LOG.error("FATAL: Cannot find service/serviceDef to use for retrieving userstore. Will NOT be able to retrieve userstore.");
        }
    }

    @Override
    public RangerUserStore retrieveUserStoreInfo(long lastKnownVersion, long lastActivationTimeInMillis) throws Exception {

        RangerUserStore rangerUserStore = null;

        if (atlasAuthAdminClient != null) {
            try {
                rangerUserStore = atlasAuthAdminClient.getUserStoreIfUpdated(lastActivationTimeInMillis);
            } catch (ClosedByInterruptException closedByInterruptException) {
                LOG.error("UserStore-retriever thread was interrupted while blocked on I/O");
                throw new InterruptedException();
            } catch (Exception e) {
                LOG.error("UserStore-retriever encounterd exception, exception=", e);
                LOG.error("Returning null userstore info");
            }
        }
        return rangerUserStore;
    }

}

