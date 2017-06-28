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
package org.apache.atlas.odf.api.discoveryservice;

import java.util.concurrent.ExecutorService;

import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.api.annotation.AnnotationStore;
import org.apache.atlas.odf.api.discoveryservice.datasets.DataSetContainer;

/**
 * Every kind of discovery service must implement this interface
 * For java services, the executor service can be used to start / manage threads with credentials of the current ODF user
 * The metadata store can be used to access metadata required by the service.
 *
 */
public interface DiscoveryService {

	void setExecutorService(ExecutorService executorService);
	
	void setMetadataStore(MetadataStore metadataStore);
	void setAnnotationStore(AnnotationStore annotationStore);

    /**
     * Checks whether a data set can be processed by the discovery service.
     * 
     * @param dataSetContainer Data set container that contains a reference to the data set to be accessed
     * @return Status information whether access to the data set is possible or not
     */
	DataSetCheckResult checkDataSet(DataSetContainer dataSetContainer);
}
