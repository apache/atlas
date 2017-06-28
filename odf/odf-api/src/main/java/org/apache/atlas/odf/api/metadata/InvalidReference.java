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

import java.util.ArrayList;
import java.util.List;

/**
 * Helper method to handle "invalid" references. 
 * 
 * Invalid references are typically returned by the metadata store implementation to indicate that a reference (or a reference list) was not provided.
 * This could be the case, e.g., for performance reasons when finding a reference might be time consuming. 
 * In such a case the application should explicitly use the MetadataQueryBuilder to get to the reference (list).
 * 
 * Clients should check any MetadataObjectReference and List<MetadataObjectRefernce> 
 * in retrieved MetadataObjects if the value is an instance of this class.
 * 
 * 
 */
public class InvalidReference  {
	
	public static final String INVALID_METADATAOBJECT_REFERENCE_ID = "INVALID_METADATAOBJECT_REFERENCE_ID";
	public static final String INVALID_METADATAOBJECT_REFERENCE_LIST_ID = "INVALID_METADATAOBJECT_REFERENCE_LIST_ID";
	
	/**
	 * use this method to indicate that a reference is invalid.
	 */
	public static MetaDataObjectReference createInvalidReference(String repositoryId) {
		MetaDataObjectReference invalidRef = new MetaDataObjectReference();
		invalidRef.setRepositoryId(repositoryId);
		invalidRef.setId(INVALID_METADATAOBJECT_REFERENCE_ID);
		return invalidRef;
	}
	
	public static boolean isInvalidRef(MetaDataObjectReference ref) {
		if (ref == null) {
			return false;
		}
		return INVALID_METADATAOBJECT_REFERENCE_ID.equals(ref.getId());
	}
	
	
	/**
	 * use this method to indicate that a list of references is invalid.
	 */
	public static List<MetaDataObjectReference> createInvalidReferenceList(String repositoryId) {
		List<MetaDataObjectReference> invalidRefList = new ArrayList<>();
		MetaDataObjectReference invalidRefMarker = new MetaDataObjectReference();
		invalidRefMarker.setRepositoryId(repositoryId);
		invalidRefMarker.setId(INVALID_METADATAOBJECT_REFERENCE_LIST_ID);
		invalidRefList.add(invalidRefMarker);
		return invalidRefList;
	}
	
	public static boolean isInvalidRefList(List<MetaDataObjectReference> refList) {
		if (refList.size() != 1) {
			return false;
		}
		MetaDataObjectReference ref = refList.get(0);
		if (ref == null) {
			return false;
		}
		return INVALID_METADATAOBJECT_REFERENCE_LIST_ID.equals(ref.getId());
	}

}
