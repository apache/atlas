/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.metadata.bridge.module;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.metadata.RepositoryMetadataModule;
import org.apache.hadoop.metadata.bridge.ABridge;
import org.apache.hadoop.metadata.bridge.BridgeManager;
import org.apache.hadoop.metadata.bridge.BridgeTypeBootstrapper;
import org.apache.hadoop.metadata.bridge.IBridge;
import org.apache.hadoop.metadata.bridge.hivelineage.HiveLineageBridge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Scope;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;

public class BridgeModule extends AbstractModule {
	public static final Logger LOG = LoggerFactory
			.getLogger(BridgeModule.class);

	@Override
	protected void configure() {
		install(new RepositoryMetadataModule());
		
		// make sure the BridgeTypeBootstrapper is only ever created once
		bind(BridgeTypeBootstrapper.class).in(Scopes.SINGLETON);
		
		// Load the configured bridge classes and add them to the map binder
		MapBinder<Class, IBridge> mapbinder = MapBinder.newMapBinder(binder(),
				Class.class, IBridge.class);

		String propsURI = System.getProperty("bridgeManager.propsFile",
				"bridge-manager.properties");

		List<Class<? extends IBridge>> bridges = getBridgeClasses(propsURI);
		for (Class<? extends IBridge> bridgeClass : bridges) {
			mapbinder.addBinding(bridgeClass).to(bridgeClass).in(Scopes.SINGLETON);
		}
	}

	/*
	 * Get the bridge classes from the configuration file
	 */
	private List<Class<? extends IBridge>> getBridgeClasses(
			String bridgePropFileName) {
		List<Class<? extends IBridge>> aBList = new ArrayList<Class<? extends IBridge>>();

		PropertiesConfiguration config = new PropertiesConfiguration();

		try {
			LOG.info("Loading : Active Bridge List");
			config.load(bridgePropFileName);
			String[] activeBridgeList = ((String) config
					.getProperty("BridgeManager.activeBridges")).split(",");
			LOG.info("Loaded : Active Bridge List");

			for (String s : activeBridgeList) {
				Class<? extends IBridge> bridgeCls = (Class<? extends IBridge>) Class
						.forName(s);
				aBList.add(bridgeCls);
			}

		} catch (ConfigurationException | IllegalArgumentException
				| SecurityException | ClassNotFoundException e) {
			LOG.error(e.getMessage(), e);
			e.printStackTrace();
		}

		return aBList;
	}
}
