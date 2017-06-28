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
package org.apache.atlas.odf.core.controlcenter;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.apache.atlas.odf.api.annotation.AnnotationStore;
import org.apache.atlas.odf.api.discoveryservice.DataSetCheckResult;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceRequest;
import org.apache.atlas.odf.api.discoveryservice.datasets.DataSetContainer;
import org.apache.atlas.odf.api.discoveryservice.sync.DiscoveryServiceSyncResponse;
import org.apache.atlas.odf.api.discoveryservice.sync.SyncDiscoveryService;
import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.core.ODFInternalFactory;

public class TransactionSyncDiscoveryServiceProxy implements SyncDiscoveryService {

	private SyncDiscoveryService wrappedService;

	public TransactionSyncDiscoveryServiceProxy(SyncDiscoveryService wrappedService) {
		this.wrappedService = wrappedService;
	}

	public DiscoveryServiceSyncResponse runAnalysis(final DiscoveryServiceRequest request) {
		TransactionContextExecutor transactionContextExecutor = new ODFInternalFactory().create(TransactionContextExecutor.class);
		try {
			return (DiscoveryServiceSyncResponse) transactionContextExecutor.runInTransactionContext(new Callable<Object>() {

				@Override
				public Object call() throws Exception {
					return wrappedService.runAnalysis(request);
				}
			});
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	public void setExecutorService(ExecutorService executorService) {
		wrappedService.setExecutorService(executorService);
	}

	public void setMetadataStore(MetadataStore metadataStore) {
		wrappedService.setMetadataStore(metadataStore);
	}

	public void setAnnotationStore(AnnotationStore annotationStore) {
		wrappedService.setAnnotationStore(annotationStore);
	}

	public DataSetCheckResult checkDataSet(final DataSetContainer dataSetContainer) {
		TransactionContextExecutor transactionContextExecutor = new ODFInternalFactory().create(TransactionContextExecutor.class);
		try {
			return (DataSetCheckResult) transactionContextExecutor.runInTransactionContext(new Callable<Object>() {

				@Override
				public Object call() throws Exception {
					return wrappedService.checkDataSet(dataSetContainer);
				}
			});
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
