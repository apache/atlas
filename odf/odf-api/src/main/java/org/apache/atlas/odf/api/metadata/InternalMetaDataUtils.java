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

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.atlas.odf.api.metadata.models.MetaDataObject;

/**
 * Internal metadata utilities
 * 
 */
public class InternalMetaDataUtils {
	public static final String ODF_PARENT_REFERENCE = "PARENT";
	public static final String ODF_CHILDREN_REFERENCE = "CHILDREN";

	/**
	 * Turn a list of metadata objects into a list of references to the corresponding metadata objects
	 *  
	 * @param objectList Given list of metadata objects
	 * @return Resulting list of references to the metadata objects
	 */
	public static List<MetaDataObjectReference> getReferenceList(List<MetaDataObject> objectList) {
		List<MetaDataObjectReference> result = new ArrayList<MetaDataObjectReference>();
		for (MetaDataObject obj : objectList) {
			result.add(obj.getReference());
		}
		return result;
	}

	/**
	 * Convert a list of metadata object references into a list of the corresponding metadata objects
	 *  
	 * @param referenceList Given list of metadata object references
	 * @return Resulting list metadata objects
	 */
	public static <T> List<T>  getObjectList(MetadataStore mds, List<MetaDataObjectReference> referenceList, Class<T> type) {
		List<T> result = new ArrayList<T>();
		for (MetaDataObjectReference ref : referenceList) {
			MetaDataObject obj = mds.retrieve(ref);
			if (obj != null) {
				try {
					result.add(type.cast(obj));
				} catch(ClassCastException e) {
					String errorMessage = MessageFormat.format("Metadata object with id ''{0}'' cannot be cast to type ''{1}''.", new Object[] { ref.getId(), type.getName() });
					throw new MetadataStoreException(errorMessage);
				}
			} else {
				String errorMessage = MessageFormat.format("Metadata object with reference ''{0}'' could not be retrieved from metadata store ''{1}''.", new Object[] { ref, mds.getRepositoryId() });
				throw new MetadataStoreException(errorMessage);
			}
		}
		return result;
	}

	/**
	 * Merge a set of given list of references to metadata objects into a single list.
	 *  
	 * @param refListArray Array of given lists of references
	 * @return Resulting merged list of references
	 */
	@SafeVarargs
	public static List<MetaDataObjectReference> mergeReferenceLists(List<MetaDataObjectReference>... refListArray) {
		HashMap<String, MetaDataObjectReference> referenceHashMap = new HashMap<String, MetaDataObjectReference>();
		for (List<MetaDataObjectReference> refList : refListArray) {
			if (refList != null) {
				for (MetaDataObjectReference ref : refList) {
					referenceHashMap.put(ref.getId(), ref);
				}
			}
		}
		return new ArrayList<MetaDataObjectReference>(referenceHashMap.values());
	}
}
