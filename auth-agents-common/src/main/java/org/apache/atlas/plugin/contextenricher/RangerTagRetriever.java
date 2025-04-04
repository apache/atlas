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

import org.apache.atlas.authorization.config.RangerPluginConfig;
import org.apache.atlas.plugin.model.RangerServiceDef;
import org.apache.atlas.plugin.policyengine.RangerPluginContext;
import org.apache.atlas.plugin.util.ServiceTags;

import java.util.Map;

public abstract class RangerTagRetriever {

	protected String             serviceName;
	protected RangerServiceDef   serviceDef;
	protected String             appId;
	protected RangerPluginConfig pluginConfig;
	protected RangerPluginContext pluginContext;

	public abstract void init(Map<String, String> options);

	public abstract ServiceTags retrieveTags(long lastKnownVersion, long lastActivationTimeInMillis) throws Exception;

	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	public RangerServiceDef getServiceDef() {
		return serviceDef;
	}

	public void setServiceDef(RangerServiceDef serviceDef) {
		this.serviceDef = serviceDef;
	}

	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public void setPluginConfig(RangerPluginConfig pluginConfig) { this.pluginConfig = pluginConfig; }

	public RangerPluginContext getPluginContext() {
		return pluginContext;
	}

	public void setPluginContext(RangerPluginContext pluginContext) {
		this.pluginContext = pluginContext;
	}
}
