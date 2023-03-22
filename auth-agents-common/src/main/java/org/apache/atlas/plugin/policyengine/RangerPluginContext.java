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

import org.apache.atlas.authz.admin.client.AtlasAuthAdminClient;
import org.apache.atlas.authz.admin.client.AtlasAuthRESTClient;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.atlas.admin.client.RangerAdminClient;
import org.apache.atlas.admin.client.RangerAdminRESTClient;
import org.apache.atlas.authorization.hadoop.config.RangerPluginConfig;
import org.apache.atlas.plugin.service.RangerAuthContext;
import org.apache.atlas.plugin.service.RangerAuthContextListener;

public class RangerPluginContext {
	private static final Log LOG = LogFactory.getLog(RangerPluginContext.class);

	private final RangerPluginConfig        config;
	private       RangerAuthContext         authContext;
	private       RangerAuthContextListener authContextListener;
	private 	  AtlasAuthAdminClient 		atlasAdminClient;


	public RangerPluginContext(RangerPluginConfig config) {
		this.config = config;
	}

	public RangerPluginConfig getConfig() { return  config; }

	public String getClusterName() {
		return config.getClusterName();
	}

	public String getClusterType() {
		return config.getClusterType();
	}

	public RangerAuthContext getAuthContext() { return authContext; }

	public void setAuthContext(RangerAuthContext authContext) { this.authContext = authContext; }

	public void setAuthContextListener(RangerAuthContextListener authContextListener) { this.authContextListener = authContextListener; }

	public void notifyAuthContextChanged() {
		RangerAuthContextListener authContextListener = this.authContextListener;

		if (authContextListener != null) {
			authContextListener.contextChanged();
		}
	}

	public AtlasAuthAdminClient getAtlasAuthAdminClient() {
		return atlasAdminClient;
	}

	public void setAtlasAuthAdminClient(AtlasAuthAdminClient atlasAdminClient) {
		this.atlasAdminClient = atlasAdminClient;
	}

	public AtlasAuthAdminClient createAtlasAuthAdminClient(RangerPluginConfig pluginConfig) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerBasePlugin.createAdminClient(" + pluginConfig.getServiceName() + ", " + pluginConfig.getAppId() + ", " + pluginConfig.getPropertyPrefix() + ")");
		}

		AtlasAuthAdminClient ret              = null;
		String            propertyName     = pluginConfig.getPropertyPrefix() + ".policy.source.impl";
		String            policySourceImpl = pluginConfig.get(propertyName);

		if(StringUtils.isEmpty(policySourceImpl)) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("Value for property[%s] was null or empty. Unexpected! Will use policy source of type[%s]", propertyName, RangerAdminRESTClient.class.getName()));
			}
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("Value for property[%s] was [%s].", propertyName, policySourceImpl));
			}

			try {
				@SuppressWarnings("unchecked")
				Class<AtlasAuthAdminClient> adminClass = (Class<AtlasAuthAdminClient>)Class.forName(policySourceImpl);

				ret = adminClass.newInstance();
			} catch (Exception excp) {
				LOG.error("failed to instantiate policy source of type '" + policySourceImpl + "'. Will use policy source of type '" + RangerAdminRESTClient.class.getName() + "'", excp);
			}
		}

		if(ret == null) {
			ret = new AtlasAuthRESTClient();
		}

		ret.init(pluginConfig.getServiceName(), pluginConfig.getAppId(), pluginConfig.getPropertyPrefix(), pluginConfig);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerBasePlugin.createAdminClient(" + pluginConfig.getServiceName() + ", " + pluginConfig.getAppId() + ", " + pluginConfig.getPropertyPrefix() + "): policySourceImpl=" + policySourceImpl + ", client=" + ret);
		}

		setAtlasAuthAdminClient(ret);

		return ret;
	}
}
