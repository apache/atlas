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
package org.apache.atlas.odf.api.annotation;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.metadata.MetadataStoreException;
import org.apache.atlas.odf.api.metadata.models.Annotation;
import org.apache.atlas.odf.api.metadata.models.ClassificationAnnotation;
import org.apache.atlas.odf.api.metadata.models.MetaDataObject;
import org.apache.atlas.odf.api.metadata.models.ProfilingAnnotation;
import org.apache.atlas.odf.api.metadata.models.RelationshipAnnotation;

public class AnnotationStoreUtils {

	/**
	 * Return the most recent annotations for the passed object but at most one per annotation type.
	 * Note that this might not be suitable for the semantics represented by some annotation types. 
	 */
	public static List<Annotation> getMostRecentAnnotationsByType(AnnotationStore as, MetaDataObjectReference object) {
		try {
			// Fix issue 99: only return one annotation per type
			Map<String, Annotation> mostRecentAnnotationsByType = new HashMap<>();
			Map<String, Long> typeToMaxTimestamp = new HashMap<>();
			for (Annotation annot : as.getAnnotations(object, null)) {
				Long ts = getTimestamp(annot);
				String annotType = annot.getAnnotationType();
				Long l = typeToMaxTimestamp.get(annotType);
				if (l == null) {
					l = ts;
				}
				if (l <= ts) {
					typeToMaxTimestamp.put(annotType, Long.valueOf(ts));
					mostRecentAnnotationsByType.put(annotType, annot);
				}
			}
			return new ArrayList<>(mostRecentAnnotationsByType.values());
		} catch (Exception exc) {
			throw new MetadataStoreException(exc);
		}
	}
	
	private static long getTimestamp(Annotation annot) {
		final long defaultVal = -1;
		String runId = annot.getAnalysisRun();
		int ix = runId.lastIndexOf("_");
		if (ix == -1) {
			return defaultVal;
		}
		String millis = runId.substring(ix);
		long result = defaultVal;
		try {
			result = Long.valueOf(millis);
		} catch (NumberFormatException e) {
			return defaultVal;
		}
		return result;
	}

	/**
	 * Retrieve the annotations of a meta data object for a given annotation store, and annotation type that have been created by
	 * a certain request.
	 * 
	 * @param mdo
	 * @param store
	 * @param annotationType
	 * @param requestId
	 * @return List of annotations for this mdo, annotation store, and annotation type created by the request with the ID 'requestId'
	 */
	
	public static List<Annotation> retrieveAnnotationsOfRun(MetaDataObject mdo, AnnotationStore store, String annotationType, String requestId) {
		Logger logger = Logger.getLogger(AnnotationStoreUtils.class.getName());
		List<Annotation> annotations = new ArrayList<>();
		for (Annotation annot : store.getAnnotations(mdo.getReference(), null)) {
			logger.log(Level.FINER, "Found annotation on object {0} with analysis run {1} and annotationType {2}",
					new Object[] { mdo.getReference().getId(), annot.getAnalysisRun(), annot.getAnnotationType() });
			if (annot.getAnalysisRun().equals(requestId) && annot.getAnnotationType().equals(annotationType)) {
				annotations.add(annot);
			}
		}
		return annotations;
	}

	/**
	 * For a given annotation return the reference to the annotated object. Throw a MetaDataStoreException if this reference is null. 
	 * 
	 * @param annot
	 * @return Meta data reference of annotation 'annot'
	 */
	
	public static MetaDataObjectReference getAnnotatedObject(Annotation annot) {
		MetaDataObjectReference annotRef = null;
		if (annot instanceof ProfilingAnnotation) {
			annotRef = ((ProfilingAnnotation) annot).getProfiledObject();
		} else if (annot instanceof ClassificationAnnotation) {
			annotRef = ((ClassificationAnnotation) annot).getClassifiedObject();
		} else if (annot instanceof RelationshipAnnotation) {
			
			List<MetaDataObjectReference> refs = ((RelationshipAnnotation) annot).getRelatedObjects();
			if (refs != null && refs.size() > 0) {
				annotRef = refs.get(0);
			}
		}
		if (annotRef == null) {
			String errorMessage = MessageFormat.format("The annotated object of annotation with ID ''{0}'' is null.", annot.getReference().getId());
			throw new MetadataStoreException(errorMessage);
		}
		return annotRef;
	}
	
}
