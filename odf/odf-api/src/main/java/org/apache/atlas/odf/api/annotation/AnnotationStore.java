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

import java.util.List;

import org.apache.atlas.odf.api.metadata.ExternalStore;
import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.metadata.models.Annotation;

public interface AnnotationStore extends ExternalStore {

	/**
	 * @return the reference to the object that was created or updated
	 */
	MetaDataObjectReference store(Annotation annotation);
	
	/**
	 * Get all annotations attached to the meta data object for a specific analysis request.
	 */
	List<Annotation> getAnnotations(MetaDataObjectReference object, String analysisRequestId);
	
	/**
	 * Retrieve an annotation by ID
	 */
	Annotation retrieveAnnotation(MetaDataObjectReference ref);
	
	/// internal
	void setAnalysisRun(String analysisRun);

	String getAnalysisRun();
};
