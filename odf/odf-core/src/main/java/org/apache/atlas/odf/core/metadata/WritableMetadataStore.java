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

import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.api.metadata.models.DataFileFolder;
import org.apache.atlas.odf.api.metadata.models.DataStore;
import org.apache.atlas.odf.api.metadata.models.Database;
import org.apache.atlas.odf.api.metadata.models.MetaDataObject;
import org.apache.atlas.odf.api.metadata.models.RelationalDataSet;
import org.apache.atlas.odf.api.metadata.models.Schema;

/**
 * Interface to be implemented by metadata stores that support write access, i.e. the creation of new metadata objects,
 * update of existing metadata objects, and creation of references between metadata objects. The new or updated objects
 * and references remain in a staging area until they are committed. This is necessary in order to avoid inconsistent
 * states during comprehensive write operations.  
 * 
 *
 */
public interface WritableMetadataStore extends MetadataStore {

	/**
	 * Add a new metadata object to the staging area of the metadata store.
	 * If the object already has a reference, the reference id might be changed when committing the new object.  
	 *
	 * @param metaDataObject Metadata object
	 */
	public MetaDataObjectReference createObject(MetaDataObject metaDataObject);

	/**
	 * Add an updated metadata object to the staging area of the metadata store. The object reference must point to an
	 * existing object in the metadata store.  
	 *
	 * @param metaDataObject Metadata object
	 */
	public void updateObject(MetaDataObject metaDataObject);

	/**
	 * Apply all staged changes to the metadata store.
	 *
	 */
	public void commit();

	/**
	 * Add a data file reference to an updated or new data file folder in the staging area.
	 * The new reference will be merged with existing references during the commit operation.
	 *
	 * @param folder Data file folder to add the reference to
	 * @param reference Reference of the data file to be added to the folder 
	 */
	public void addDataFileReference(DataFileFolder folder, MetaDataObjectReference reference);

	/**
	 * Add a data file folder reference to an updated or new data file folder in the staging area.
	 * The new reference will be merged with existing references during the commit operation.
	 *
	 * @param folder Data file folder to add the reference to
	 * @param reference Reference of the data file folder to be added to the folder 
	 */
	public void addDataFileFolderReference(DataFileFolder folder, MetaDataObjectReference reference);

	/**
	 * Add a schema reference to an updated or new database in the staging area.
	 * The new reference will be merged with existing references during the commit operation.
	 *
	 * @param database Database to add the reference to
	 * @param reference Reference of the schema to be added to the database 
	 */
	public void addSchemaReference(Database database, MetaDataObjectReference reference);

	/**
	 * Add a table reference to an updated or new schema in the staging area.
	 * The new reference will be merged with existing references during the commit operation.
	 *
	 * @param schema Schema to add the reference to
	 * @param reference Reference of the table to be added to the schema 
	 */
	public void addTableReference(Schema schema, MetaDataObjectReference reference);

	/**
	 * Add a column reference to an updated or new relational data set in the staging area.
	 * The new reference will be merged with existing references during the commit operation.
	 *
	 * @param relationalDataSet Relational data set to add the reference to
	 * @param reference Reference of the column to be added to the relational data set 
	 */
	public void addColumnReference(RelationalDataSet relationalDataSet, MetaDataObjectReference reference);

	/**
	 * Add a connection reference to an updated or new data store in the staging area.
	 * The new reference will be merged with existing references during the commit operation.
	 *
	 * @param dataStore Data store set to add the reference to
	 * @param reference Reference of the connection to be added to the data store 
	 */
	public void addConnectionReference(DataStore dataStore, MetaDataObjectReference reference);

}
