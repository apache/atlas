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
package org.apache.atlas.odf.core.metadata;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.metadata.InternalMetadataStoreBase;
import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.metadata.MetadataStoreException;
import org.apache.atlas.odf.api.metadata.StoredMetaDataObject;
import org.apache.atlas.odf.api.metadata.models.DataFileFolder;
import org.apache.atlas.odf.api.metadata.models.DataStore;
import org.apache.atlas.odf.api.metadata.models.Database;
import org.apache.atlas.odf.api.metadata.models.MetaDataObject;
import org.apache.atlas.odf.api.metadata.models.RelationalDataSet;
import org.apache.atlas.odf.api.metadata.models.Schema;

/**
 * Common base for writable metadata stores.
 * Note that the methods implemented by InternalMetadataStoreBase are not necessarily used by all classes that extend WritableMetadataStoreBase.
 * (If Java supported multiple inheritance, WritableMetadataStoreBase and InternalMetadataStoreBase would be independent classes.)
 * 
 * 
 */
public abstract class WritableMetadataStoreBase extends InternalMetadataStoreBase implements WritableMetadataStore {
	private static Logger logger = Logger.getLogger(WritableMetadataStoreBase.class.getName());

	abstract protected LinkedHashMap<String, StoredMetaDataObject> getStagedObjects();

	private void addReference(MetaDataObject metaDataObject, String attributeName, MetaDataObjectReference reference) {
		if (metaDataObject.getReference() == null) {
			throw new MetadataStoreException("Cannot add a reference because metadata object reference is null.");
		}
		StoredMetaDataObject obj = this.getStagedObjects().get(metaDataObject.getReference().getId());
		if (obj != null) {
			if (obj.getReferenceMap().get(attributeName) == null) {
				obj.getReferenceMap().put(attributeName, new ArrayList<MetaDataObjectReference>());
			}
			obj.getReferenceMap().get(attributeName).add(reference);
		} else {
			String errorMessage = MessageFormat.format("A staged object with id ''{0}'' does not exist. Create or update the object before adding a reference.", metaDataObject.getReference().getId());
			throw new MetadataStoreException(errorMessage);
		}
	}

	@Override
	public void addDataFileReference(DataFileFolder folder, MetaDataObjectReference reference) {
		addReference(folder, ODF_DATAFILES_REFERENCE, reference);
	}

	@Override
	public void addDataFileFolderReference(DataFileFolder folder, MetaDataObjectReference reference) {
		addReference(folder, ODF_DATAFILEFOLDERS_REFERENCE, reference);
	}

	@Override
	public void addSchemaReference(Database database, MetaDataObjectReference reference) {
		addReference(database, ODF_SCHEMAS_REFERENCE, reference);
	}

	@Override
	public void addTableReference(Schema schema, MetaDataObjectReference reference) {
		addReference(schema, ODF_TABLES_REFERENCE, reference);
	}

	@Override
	public void addColumnReference(RelationalDataSet relationalDataSet, MetaDataObjectReference reference) {
		addReference(relationalDataSet, ODF_COLUMNS_REFERENCE, reference);
	}

	@Override
	public void addConnectionReference(DataStore dataStore, MetaDataObjectReference reference) {
		addReference(dataStore, ODF_CONNECTIONS_REFERENCE, reference);
	}

	@Override
	public MetaDataObjectReference createObject(MetaDataObject metaDataObject) {
		if (metaDataObject.getReference() == null) {
			metaDataObject.setReference(WritableMetadataStoreUtils.generateMdoRef(this));
		}
		this.getStagedObjects().put(metaDataObject.getReference().getId(), new StoredMetaDataObject(metaDataObject));
		logger.log(Level.FINE, "Added new new object of type ''{0}'' with id ''{1}'' to staging area.",
				new Object[] { metaDataObject.getClass().getSimpleName(), metaDataObject.getReference().getId() });
		return metaDataObject.getReference();
	}

	@Override
	public void updateObject(MetaDataObject metaDataObject) {
		if (metaDataObject.getReference() == null) {
			throw new MetadataStoreException("Reference attribute cannot be ''null'' when updating a metadata object.");
		}
		if (retrieve(metaDataObject.getReference()) == null) {
			throw new MetadataStoreException(
					MessageFormat.format("An object wih id ''{0}'' does not extist in this metadata store.",
							metaDataObject.getReference().getId()));
		}
		this.getStagedObjects().put(metaDataObject.getReference().getId(), new StoredMetaDataObject(metaDataObject));
		logger.log(Level.FINE, "Added updated object of type ''{0}'' with id ''{1}'' to staging area.",
				new Object[] { metaDataObject.getClass().getSimpleName(), metaDataObject.getReference().getId() });
	}

}
