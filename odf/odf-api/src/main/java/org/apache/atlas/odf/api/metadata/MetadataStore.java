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

import java.util.List;

import org.apache.atlas.odf.api.metadata.models.Column;
import org.apache.atlas.odf.api.metadata.models.Connection;
import org.apache.atlas.odf.api.metadata.models.ConnectionInfo;
import org.apache.atlas.odf.api.metadata.models.DataFile;
import org.apache.atlas.odf.api.metadata.models.DataFileFolder;
import org.apache.atlas.odf.api.metadata.models.DataStore;
import org.apache.atlas.odf.api.metadata.models.Database;
import org.apache.atlas.odf.api.metadata.models.MetaDataObject;
import org.apache.atlas.odf.api.metadata.models.RelationalDataSet;
import org.apache.atlas.odf.api.metadata.models.Schema;
import org.apache.atlas.odf.api.metadata.models.Table;

/**
 * Interfaces to be implemented by a metadata store in order to be used with ODF.
 * 
 * In addition to this interface, each ODF metadata store must support the ODF base types defined by the
 * {@link WritableMetadataStoreUtils#getBaseTypes} method.
 *
 */
public interface MetadataStore extends ExternalStore {

	/**
	 * Retrieve information required to access the actual data behind an information asset, e.g. the connection info
	 * to retrieve the data in a JDBC table.
	 *  
	 * @param informationAsset Given information asset
	 * @return Connection information required for data access
	 */
	ConnectionInfo getConnectionInfo(MetaDataObject informationAsset);

	/**
	 * Retrieve a metadata object by its metadata object reference.
	 *  
	 * @param reference Metadata object reference
	 * @return Metadata object
	 */
	MetaDataObject retrieve(MetaDataObjectReference reference);
	
	/**
	 * Perform a search against the metadata store. The query should be generated using the {@link MetadataQueryBuilder}
	 * returned by the {@link #newQueryBuilder()} method.

	 * @param query Query string
	 * @return List of references to metadata objects found by the query
	 */
	List<MetaDataObjectReference> search(String query);
	
	/**
	 * Populates the metadata store with example datasets. This method is optional, however in order to support the ODF
	 * integration tests, this method must create the object returned by the {@link WritableMetadataStoreUtils#getSampleDataObjects}
	 * method.
	 * 
	 */
	void createSampleData();

	/**
	 * Deletes all data from this repository. This method is optional, however it must be implemented in order to support the ODF
	 * integration tests.
	 * 
	 */
	void resetAllData();
	
	MetadataQueryBuilder newQueryBuilder();
	
	/**
	 * Return an implementation of the {@link AnnotationPropagator} interface that propagates ODF annotations into the metadata store.
	 * The method may return null if the metadata store does not support annotation propagation.
	 * 
	 * return the AnnotationPropagator for this MetadataStore.
	 */
	AnnotationPropagator getAnnotationPropagator();

	/**
	 * Retrieve references of a specific type from an object stored in the metadata store.
	 * A list of available reference types can be retrieved with the {@link #getReferenceTypes() getReferenceTypes} method.
	 *  
	 * @param metaDataObject Given metadata object to retrieve the references from
	 * @param attributeName Name of the reference
	 * @return List of objects referenced by the given metadata object
	 */
	public List<MetaDataObject> getReferences(String attributeName, MetaDataObject metaDataObject);

	/**
	 * Return the list of available reference types supported by the {@link #getReferences(String, MetaDataObject) getReferences} method of the metadata store.
	 * The list indicates which reference types are added to the internal metadata cache when a discovery service is called. That way, they will be available
	 * to the service at runtime even if the service has no access to the metadata store.
	 *  
	 * @return List of supported reference types 
	 */
	public List<String> getReferenceTypes();

	/**
	 * Retrieve the parent object of a given object stored in the metadata store.
	 *  
	 * @param metaDataObject Given metadata object
	 * @return Parent object of the metadata object
	 */
	public MetaDataObject getParent(MetaDataObject metaDataObject);

	/**
	 * Retrieve the child objects of a given object stored in the metadata store.
	 *  
	 * @param metaDataObject Given metadata object
	 * @return List of child objects objects referenced by the given metadata object
	 */
	public List<MetaDataObject> getChildren(MetaDataObject metaDataObject);

	/**
	 * Retrieve data file objects referenced by a data file folder object.
	 *  
	 * @param metaDataObject Given metadata object
	 * @return List of data file objects
	 */
	public List<DataFile> getDataFiles(DataFileFolder folder);

	/**
	 * Retrieve data file folder objects referenced by a data file folder object.
	 *  
	 * @param metaDataObject Given metadata object
	 * @return List of data file folder objects
	 */
	public List<DataFileFolder> getDataFileFolders(DataFileFolder folder);

	/**
	 * Retrieve schema objects referenced by a database object.
	 *  
	 * @param metaDataObject Given metadata object
	 * @return List of schema objects
	 */
	public List<Schema> getSchemas(Database database);

	/**
	 * Retrieve table objects referenced by a schema object.
	 *  
	 * @param metaDataObject Given metadata object
	 * @return List of table objects
	 */
	public List<Table> getTables(Schema schema);

	/**
	 * Retrieve column objects referenced by a table object.
	 *  
	 * @param metaDataObject Given metadata object
	 * @return List of column objects
	 */
	public List<Column> getColumns(RelationalDataSet relationalDataSet);

	/**
	 * Retrieve connection objects referenced by a data store object.
	 *  
	 * @param metaDataObject Given metadata object
	 * @return List of connection objects
	 */
	public List<Connection> getConnections(DataStore dataStore);

}
