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
package org.apache.atlas.odf.api.metadata.models;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.metadata.InternalMetadataStoreBase;
import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.api.metadata.AnnotationPropagator;
import org.apache.atlas.odf.api.metadata.InternalMetaDataUtils;
import org.apache.atlas.odf.api.metadata.StoredMetaDataObject;

/**
 * In-memory metadata cache to be used by discovery services that do not have access to the metadata store.
 * The cache uses the same interface as the metadata store but does not support all of its methods.
 * 
 * 
 */
public class CachedMetadataStore extends InternalMetadataStoreBase implements MetadataStore {
	private Logger logger = Logger.getLogger(CachedMetadataStore.class.getName());
	private static final String METADATA_STORE_ID = "ODF_METADATA_CACHE";
	private static final String STORE_PROPERTY_TYPE = "cache";
	private static final String STORE_PROPERTY_DESCRIPTION = "ODF metadata cache";

	protected Object accessLock = new Object();
	private HashMap<String, StoredMetaDataObject> objectStore =  new HashMap<String, StoredMetaDataObject>();
	private HashMap<String, ConnectionInfo> connectionInfoStore = new HashMap<String, ConnectionInfo>();

	public CachedMetadataStore(MetaDataCache metaDataCache) {
		for (StoredMetaDataObject obj : metaDataCache.getMetaDataObjects()) {
			getObjects().put(obj.getMetaDataObject().getReference().getId(), obj);
			logger.log(Level.FINER, "Added object with name ''{0}'' to metadata cache.", obj.getMetaDataObject().getName());
		}
		for (ConnectionInfo conInfo : metaDataCache.getConnectionInfoObjects()) {
			connectionInfoStore.put(conInfo.getAssetReference().getId(), conInfo);
			logger.log(Level.FINER, "Added connection info object for metadata object id ''{0}'' to metadata cache.", conInfo.getAssetReference().getId());
		}
	}

	protected Object getAccessLock() {
		return accessLock;
	}

	public static MetaDataCache retrieveMetaDataCache(MetadataStore mds, MetaDataObject metaDataObject) {
		MetaDataCache cache = new MetaDataCache();
		populateMetaDataCache(cache, mds, metaDataObject);
		return cache;
	}
	/**
	 * Internal methods that recursively populates the metadata store with all child objects of a given metadata object.
	 * If there is a @see ConnectionInfo object available for a cached metadata object
	 * it will be added to the cache as well.
	 *  
	 * @param metaDataCache Metadata cache to be populated
	 * @param mds Metadata store to retrieve the cached objects from
	 * @param metaDataObject Given metadata object
	 */
	private static void populateMetaDataCache(MetaDataCache metaDataCache, MetadataStore mds, MetaDataObject metaDataObject) {
		// Add current object
		StoredMetaDataObject currentObject = new StoredMetaDataObject(metaDataObject);
		for (String referenceType : mds.getReferenceTypes()) {
			currentObject.getReferenceMap().put(referenceType, InternalMetaDataUtils.getReferenceList(mds.getReferences(referenceType, metaDataObject)));
		}
		metaDataCache.getMetaDataObjects().add(currentObject);
		ConnectionInfo connectionInfo = mds.getConnectionInfo(metaDataObject);

		// Connection info must be cached as well because it cannot be retrieved dynamically as required parent objects might be missing from cache
		if (connectionInfo != null) {
			metaDataCache.getConnectionInfoObjects().add(connectionInfo);
		}

		// Add child objects
		for (MetaDataObject child : mds.getChildren(metaDataObject)) {
			populateMetaDataCache(metaDataCache, mds, child);
		}
	}

	protected HashMap<String, StoredMetaDataObject> getObjects() {
		return objectStore;
	}

	@Override
	public Properties getProperties() {
		Properties props = new Properties();
		props.put(MetadataStore.STORE_PROPERTY_DESCRIPTION, STORE_PROPERTY_DESCRIPTION);
		props.put(MetadataStore.STORE_PROPERTY_TYPE, STORE_PROPERTY_TYPE);
		props.put(STORE_PROPERTY_ID, METADATA_STORE_ID);
		return props;
	}

	@Override
	public void resetAllData() {
		throw new UnsupportedOperationException("Method not available in this implementation of the Metadata store.");
	}

	@Override
	public String getRepositoryId() {
		return METADATA_STORE_ID;
	}

	@Override
	public ConnectionInfo getConnectionInfo(MetaDataObject metaDataObject) {
		return connectionInfoStore.get(metaDataObject.getReference().getId());
	}

	@Override
	public List<MetaDataObjectReference> search(String query) {
		throw new UnsupportedOperationException("Method not available in this implementation of the Metadata store.");
	}

	@Override
	public void createSampleData() {
		throw new UnsupportedOperationException("Method not available in this implementation of the Metadata store.");
	}

	@Override
	public AnnotationPropagator getAnnotationPropagator() {
		throw new UnsupportedOperationException("Method not available in this implementation of the Metadata store.");
	}

}
