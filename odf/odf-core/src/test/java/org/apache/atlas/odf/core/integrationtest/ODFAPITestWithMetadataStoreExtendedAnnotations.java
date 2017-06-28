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
package org.apache.atlas.odf.core.integrationtest;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.api.metadata.models.Annotation;
import org.apache.atlas.odf.api.analysis.AnalysisRequestStatus.State;
import org.apache.atlas.odf.api.annotation.AnnotationStore;
import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.core.test.annotation.TestSyncDiscoveryServiceWritingExtendedAnnotations.MyObject;
import org.apache.atlas.odf.core.test.annotation.TestSyncDiscoveryServiceWritingExtendedAnnotations.MyOtherObject;
import org.apache.atlas.odf.core.test.annotation.TestSyncDiscoveryServiceWritingExtendedAnnotations.SyncDiscoveryServiceAnnotation;
import org.apache.atlas.odf.json.JSONUtils;

public class ODFAPITestWithMetadataStoreExtendedAnnotations extends ODFAPITestWithMetadataStoreBase {

	@Test
	public void testSuccessSyncExtendedAnnotations() throws Exception {
		MetadataStore mds = new ODFFactory().create().getMetadataStore();
		AnnotationStore as = new ODFFactory().create().getAnnotationStore();
		List<MetaDataObjectReference> dataSets = getTables(mds);
		String dsID = "synctestservice-with-extendedannotations";

		String requestId = test(dsID, dataSets, State.FINISHED, false, null);

		log.info("Checking if extended annotations exist for request ID: " + requestId);
		for (MetaDataObjectReference dataSet : dataSets) {
			List<SyncDiscoveryServiceAnnotation> annotations = new ArrayList<>();
			List<Annotation> annots = as.getAnnotations(dataSet, null);
			Assert.assertTrue(annots.size() >= 2);
			
			for (Annotation annot : annots) {		
				Assert.assertNotNull(annot);
				if (annot.getAnalysisRun().equals(requestId)) {
					log.info("Found annotation: " + annot + ", json: " + JSONUtils.toJSON(annot));
					Assert.assertNotNull(annot);
					Assert.assertEquals(SyncDiscoveryServiceAnnotation.class, annot.getClass());
					SyncDiscoveryServiceAnnotation extAnnot = (SyncDiscoveryServiceAnnotation) annot;
					Assert.assertNotNull(extAnnot.getProp1());
					Assert.assertEquals(extAnnot.getProp1().hashCode(), extAnnot.getProp2());
					MyObject mo = extAnnot.getProp3();
					Assert.assertNotNull(mo);
					Assert.assertEquals("nested" + extAnnot.getProp1(), mo.getAnotherProp());
					
					MyOtherObject moo = mo.getYetAnotherProp();
					Assert.assertNotNull(moo);
					Assert.assertEquals("nestedtwolevels" + extAnnot.getProp1(), moo.getMyOtherObjectProperty());
					annotations.add(extAnnot);
				}
			}
			Assert.assertEquals(2, annotations.size());
			// TODO check annotations list
		}
	}

}
