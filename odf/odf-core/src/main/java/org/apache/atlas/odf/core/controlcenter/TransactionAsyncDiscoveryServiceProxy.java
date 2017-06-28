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
import org.apache.atlas.odf.api.discoveryservice.async.AsyncDiscoveryService;
import org.apache.atlas.odf.api.discoveryservice.async.DiscoveryServiceAsyncRunStatus;
import org.apache.atlas.odf.api.discoveryservice.async.DiscoveryServiceAsyncStartResponse;
import org.apache.atlas.odf.api.discoveryservice.datasets.DataSetContainer;
import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.core.ODFInternalFactory;

public class TransactionAsyncDiscoveryServiceProxy implements AsyncDiscoveryService {

	private AsyncDiscoveryService wrappedService;

	public TransactionAsyncDiscoveryServiceProxy(AsyncDiscoveryService wrappedService) {
		this.wrappedService = wrappedService;
	}

	public DiscoveryServiceAsyncStartResponse startAnalysis(final DiscoveryServiceRequest request) {
		TransactionContextExecutor transactionContextExecutor = new ODFInternalFactory().create(TransactionContextExecutor.class);
		try {
			return (DiscoveryServiceAsyncStartResponse) transactionContextExecutor.runInTransactionContext(new Callable<Object>() {

				@Override
				public Object call() throws Exception {
					return wrappedService.startAnalysis(request);
				}
			});
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	public DiscoveryServiceAsyncRunStatus getStatus(final String runId) {
		TransactionContextExecutor transactionContextExecutor = new ODFInternalFactory().create(TransactionContextExecutor.class);
		try {
			return (DiscoveryServiceAsyncRunStatus) transactionContextExecutor.runInTransactionContext(new Callable<Object>() {

				@Override
				public Object call() throws Exception {
					return wrappedService.getStatus(runId);
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
