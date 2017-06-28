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
package org.apache.atlas.odf.core.annotation;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.metadata.models.Annotation;
import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.analysis.AnalysisRequest;
import org.apache.atlas.odf.api.annotation.AnnotationStore;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceResult;

public class InternalAnnotationStoreUtils {

	public static void storeDiscoveryServiceResult(DiscoveryServiceResult result, AnalysisRequest req) {
		Logger logger = Logger.getLogger(InternalAnnotationStoreUtils.class.getName());
		AnnotationStore mds = new ODFFactory().create().getAnnotationStore();
		mds.setAnalysisRun(req.getId());
		if (result != null) {
			logger.log(Level.FINE, "Persisting annotations returned by discovery service");
			List<Annotation> annotations = result.getAnnotations();
			if (annotations != null) {
				for (Annotation annot : annotations) {
					// only persist if reference was not set
					if (annot.getReference() == null) {
						mds.store(annot);
					} else {
						logger.log(Level.WARNING, "Returned annotation object has a non-null reference set and will not be persisted (reference: {0})", annot.getReference().toString());
					}
				}
			}
		}
	}

}
