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
package org.apache.atlas.odf.api.metadata;

import org.apache.atlas.odf.api.annotation.AnnotationStore;

/**
 * Interface for the logic that propagates annotations from the AnnotationStore to the MetadataStore 
 *
 */
public interface AnnotationPropagator {

	/**
	 * Run the actual propagation process 
	 *@param as The annotation store from which the annotations should be taken from
	 *@param requestId Propagate only annotations that belong to a specific analysis request id (optional)
	 * 
	 */
	void propagateAnnotations(AnnotationStore as, String requestId);
}
