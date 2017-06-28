/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.odf.core;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.OpenDiscoveryFramework;
import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.api.metadata.importer.JDBCMetadataImporter;
import org.apache.atlas.odf.api.settings.SettingsManager;
import org.apache.wink.json4j.JSONException;

import org.apache.atlas.odf.api.analysis.AnalysisManager;
import org.apache.atlas.odf.api.annotation.AnnotationStore;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceManager;
import org.apache.atlas.odf.api.engine.EngineManager;
import org.apache.atlas.odf.api.engine.ServiceRuntimesInfo;
import org.apache.atlas.odf.core.controlcenter.ServiceRuntimes;
import org.apache.atlas.odf.json.JSONUtils;

public class OpenDiscoveryFrameworkImpl implements OpenDiscoveryFramework {

	private Logger logger = Logger.getLogger(OpenDiscoveryFrameworkImpl.class.getName());

	public OpenDiscoveryFrameworkImpl() {
		if (!ODFInitializer.isRunning() && !ODFInitializer.isStartStopInProgress()) {
			logger.log(Level.INFO, "Initializing Open Discovery Platform");
			ODFInitializer.start();
			getEngineManager().checkHealthStatus(); // This implicitly initializes the control center and the message queues
			
			logger.log(Level.INFO, "Open Discovery Platform successfully initialized.");
			
			// log active runtimes
			ServiceRuntimesInfo activeRuntimesInfo = ServiceRuntimes.getRuntimesInfo(ServiceRuntimes.getActiveRuntimes());
			try {
				logger.log(Level.INFO, "Active runtimes: ''{0}''", JSONUtils.toJSON(activeRuntimesInfo));
			} catch (JSONException e) {
				logger.log(Level.WARNING, "Active runtime info has wrong format", e);
			}
		}
	}

	public AnalysisManager getAnalysisManager() {
		return new ODFInternalFactory().create(AnalysisManager.class);
	}

	public DiscoveryServiceManager getDiscoveryServiceManager() {
		return new ODFInternalFactory().create(DiscoveryServiceManager.class);
	}

	public EngineManager getEngineManager() {
		return new ODFInternalFactory().create(EngineManager.class);
	}

	public SettingsManager getSettingsManager() {
		return new ODFInternalFactory().create(SettingsManager.class);
	}

	public AnnotationStore getAnnotationStore() {
		return new ODFInternalFactory().create(AnnotationStore.class);
	}

	public MetadataStore getMetadataStore() {
		return new ODFInternalFactory().create(MetadataStore.class);
	}

	public JDBCMetadataImporter getJDBCMetadataImporter() {
		return new ODFInternalFactory().create(JDBCMetadataImporter.class);
	}
}
