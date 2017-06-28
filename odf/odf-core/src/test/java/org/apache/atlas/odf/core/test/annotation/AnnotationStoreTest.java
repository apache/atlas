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

import java.util.List;
import java.util.UUID;

import org.apache.atlas.odf.api.metadata.models.Annotation;
import org.apache.atlas.odf.api.metadata.models.ProfilingAnnotation;
import org.junit.Assert;
import org.junit.Test;

import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.annotation.AnnotationStore;
import org.apache.atlas.odf.core.controlcenter.DefaultStatusQueueStore;
import org.apache.atlas.odf.core.test.ODFTestcase;

public class AnnotationStoreTest extends ODFTestcase {

	private AnnotationStore createAnnotationStore() {
		return new DefaultStatusQueueStore();
	}
	
	@Test
	public void testStoreProfilingAnnotation() throws Exception {
		AnnotationStore as = createAnnotationStore();
		
		String modRef1Id = UUID.randomUUID().toString();
		MetaDataObjectReference mdoref1 = new MetaDataObjectReference();
		mdoref1.setId(modRef1Id);
		
		ProfilingAnnotation annot1 = new ProfilingAnnotation();
		annot1.setJsonProperties("{\"a\": \"b\"}");
		annot1.setAnnotationType("AnnotType1");
		annot1.setProfiledObject(mdoref1);

		MetaDataObjectReference annot1Ref = as.store(annot1);
		Assert.assertNotNull(annot1Ref.getId());
		List<Annotation> retrievedAnnots = as.getAnnotations(mdoref1, null);
		Assert.assertEquals(1, retrievedAnnots.size());
		
		Annotation retrievedAnnot = retrievedAnnots.get(0);
		Assert.assertTrue(annot1 != retrievedAnnot);
		Assert.assertTrue(retrievedAnnot instanceof ProfilingAnnotation);
		ProfilingAnnotation retrievedProfilingAnnotation = (ProfilingAnnotation) retrievedAnnot;
		Assert.assertEquals(modRef1Id, retrievedProfilingAnnotation.getProfiledObject().getId());
		Assert.assertEquals(annot1Ref, retrievedAnnot.getReference());
		
	}

}
