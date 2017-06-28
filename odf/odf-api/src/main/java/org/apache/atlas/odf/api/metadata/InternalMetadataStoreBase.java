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
 * Common base for default metadata store and metadata cache.
 * 
 * 
 */
public abstract class InternalMetadataStoreBase extends MetadataStoreBase implements MetadataStore {

	protected abstract HashMap<String, StoredMetaDataObject> getObjects();

	protected <T> List<T> getReferences(String attributeName, MetaDataObject metaDataObject, Class<T> type) {
		if ((metaDataObject == null) || (metaDataObject.getReference() == null)) {
			throw new MetadataStoreException("Metadata object or its reference attribute cannot be null.");
		}
		List<T> result = new ArrayList<T>();
		StoredMetaDataObject internalObj = getObjects().get(metaDataObject.getReference().getId());
		if ((internalObj != null) && (internalObj.getReferenceMap().get(attributeName) != null)) {
			for (MetaDataObjectReference ref : internalObj.getReferenceMap().get(attributeName)) {
				MetaDataObject obj = retrieve(ref);
				if (obj != null) {
					// Ignore objects that are not available in metadata store
					// TODO: Consider to use invalide reference if an object is not available
					try {
						result.add(type.cast(retrieve(ref)));
					} catch(ClassCastException e) {
						String errorMessage = MessageFormat.format("Inconsistent object reference: A reference of type ''{0}'' cannot be cast to type ''{1}''.", new Object[] { attributeName, type.getName() });
						throw new MetadataStoreException(errorMessage);
					}
				}
			}
		}
		return result;
	}

	abstract protected Object getAccessLock();

	@Override
	public MetaDataObject getParent(MetaDataObject metaDataObject) {
		List<MetaDataObject> parentList = new ArrayList<MetaDataObject>();
		// TODO: Make this more efficient
		for (StoredMetaDataObject internalMdo : getObjects().values()) {
			for (MetaDataObject child : getChildren(internalMdo.getMetaDataObject())) {
				if (child.getReference().getId().equals(metaDataObject.getReference().getId())) {
					parentList.add(internalMdo.getMetaDataObject());
				}
			}
		}
		if (parentList.size() == 1) {
			return parentList.get(0);
		} else if (parentList.size() == 0) {
			return null;
		}
		String errorMessage = MessageFormat.format("Inconsistent object reference: Metadata object with id ''{0}'' refers to more that one parent object.", metaDataObject.getReference().getId());
		throw new MetadataStoreException(errorMessage);
	}

	@Override
	public MetaDataObject retrieve(MetaDataObjectReference reference) {
		synchronized(getAccessLock()) {
			String objectId = reference.getId();
			if (getObjects().containsKey(objectId)) {
				return getObjects().get(objectId).getMetaDataObject();
			}
			return null;
		}
	}

	@Override
	public MetadataQueryBuilder newQueryBuilder() {
		return new DefaultMetadataQueryBuilder();
	}
}
