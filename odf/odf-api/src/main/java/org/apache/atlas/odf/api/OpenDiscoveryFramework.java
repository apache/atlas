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
package org.apache.atlas.odf.api;

import org.apache.atlas.odf.api.analysis.AnalysisManager;
import org.apache.atlas.odf.api.annotation.AnnotationStore;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceManager;
import org.apache.atlas.odf.api.engine.EngineManager;
import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.api.metadata.importer.JDBCMetadataImporter;
import org.apache.atlas.odf.api.settings.SettingsManager;

/**
*
* External Java API for managing and controlling ODF
*
*/
public interface OpenDiscoveryFramework {

	/**
	 * Returns API for managing ODF analysis requests
	 *
	 * @return ODF analysis manager API
	 */
	public AnalysisManager getAnalysisManager();

	/**
	 * Returns API for managing ODF discovery services
	 *
	 * @return ODF discovery services manager API
	 */
	public DiscoveryServiceManager getDiscoveryServiceManager();

	/**
	 * Returns API for controlling the ODF engine
	 *
	 * @return ODF engine manager API
	 */
	public EngineManager getEngineManager();

	/**
	 * Returns API for managing ODF settings
	 *
	 * @return ODF settings manager API
	 */
	public SettingsManager getSettingsManager();

	/**
	 * Returns ODF annotation store API
	 *
	 * @return ODF annotation store API
	 */
	public AnnotationStore getAnnotationStore();

	/**
	 * Returns ODF metadata store API
	 *
	 * @return ODF metadata store API
	 */
	public MetadataStore getMetadataStore();

	/**
	 * Returns JDBC importer utility for populating the metadata store with sample data
	 *
	 * @return ODF JDBC importer utility
	 */
	public JDBCMetadataImporter getJDBCMetadataImporter();
}
