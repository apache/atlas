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
package org.apache.atlas.odf.core.test.annotation;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.metadata.models.Annotation;
import org.apache.atlas.odf.api.metadata.models.ProfilingAnnotation;
import org.apache.atlas.odf.core.Utils;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceBase;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceRequest;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceResult;
import org.apache.atlas.odf.api.discoveryservice.sync.DiscoveryServiceSyncResponse;
import org.apache.atlas.odf.api.discoveryservice.sync.SyncDiscoveryService;
import org.apache.atlas.odf.core.test.ODFTestLogger;
import org.apache.atlas.odf.json.JSONUtils;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceResponse;

public class TestSyncDiscoveryServiceWritingJsonAnnotations extends DiscoveryServiceBase implements SyncDiscoveryService {
	Logger logger = ODFTestLogger.get();
	private String annotationResult = Utils.getInputStreamAsString(this.getClass().getClassLoader().getResourceAsStream("org/apache/atlas/odf/core/integrationtest/metadata/internal/atlas/nested_annotation_example.json"), "UTF-8");

	@Override
	public DiscoveryServiceSyncResponse runAnalysis(DiscoveryServiceRequest request) {
		try {
			MetaDataObjectReference dataSetRef = request.getDataSetContainer().getDataSet().getReference();

			List<Annotation> annotations = new ArrayList<>();
			ProfilingAnnotation annotation1 = new ProfilingAnnotation();
			annotation1.setProfiledObject(dataSetRef);
			annotation1.setJsonProperties(annotationResult);
			annotation1.setAnnotationType("JsonAnnotationWriteTest");
			annotation1.setJavaClass("JsonAnnotationWriteTest");
			annotations.add(annotation1);

			DiscoveryServiceSyncResponse resp = new DiscoveryServiceSyncResponse();
			resp.setCode(DiscoveryServiceResponse.ResponseCode.OK);
			DiscoveryServiceResult dsResult = new DiscoveryServiceResult();
			dsResult.setAnnotations(annotations);
			resp.setResult(dsResult);
			resp.setDetails(this.getClass().getName() + ".runAnalysis finished OK");

			logger.info("Returning from discovery service " + this.getClass().getSimpleName() + " with result: " + JSONUtils.toJSON(resp));
			return resp;
		} catch (Exception exc) {
			throw new RuntimeException(exc);
		}
	}
}
