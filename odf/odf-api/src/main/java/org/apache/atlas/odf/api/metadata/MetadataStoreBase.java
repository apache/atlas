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

import org.apache.atlas.odf.api.metadata.models.Column;
import org.apache.atlas.odf.api.metadata.models.Connection;
import org.apache.atlas.odf.api.metadata.models.DataFile;
import org.apache.atlas.odf.api.metadata.models.DataFileFolder;
import org.apache.atlas.odf.api.metadata.models.DataStore;
import org.apache.atlas.odf.api.metadata.models.Database;
import org.apache.atlas.odf.api.metadata.models.MetaDataObject;
import org.apache.atlas.odf.api.metadata.models.RelationalDataSet;
import org.apache.atlas.odf.api.metadata.models.Schema;
import org.apache.atlas.odf.api.metadata.models.Table;

/**
 * Common base that may be used for any metadata store implementation.
 * 
 * 
 */
public abstract class MetadataStoreBase implements MetadataStore {

	public static final String ODF_CONNECTIONS_REFERENCE = "CONNECTIONS";
	public static final String ODF_COLUMNS_REFERENCE = "COLUMNS";
	public static final String ODF_DATAFILEFOLDERS_REFERENCE = "DATAFILEFOLDERS";
	public static final String ODF_DATAFILES_REFERENCE = "DATAFILES";
	public static final String ODF_SCHEMAS_REFERENCE = "SCHEMAS";
	public static final String ODF_TABLES_REFERENCE = "TABLES";

	protected abstract <T> List<T> getReferences(String attributeName, MetaDataObject metaDataObject, Class<T> type);

	@Override
	public List<String> getReferenceTypes() {
		List<String> result = new ArrayList<String>();
		result.add(ODF_CONNECTIONS_REFERENCE);
		result.add(ODF_COLUMNS_REFERENCE);
		result.add(ODF_DATAFILEFOLDERS_REFERENCE);
		result.add(ODF_DATAFILES_REFERENCE);
		result.add(ODF_SCHEMAS_REFERENCE);
		result.add(ODF_TABLES_REFERENCE);
		return result;
	}

	@Override
	public List<MetaDataObject> getReferences(String attributeName, MetaDataObject metaDataObject) {
		return getReferences(attributeName, metaDataObject, MetaDataObject.class);
	}

	@Override
	public List<DataFile> getDataFiles(DataFileFolder folder) {
		return getReferences(ODF_DATAFILES_REFERENCE, folder, DataFile.class);
	}

	@Override
	public List<DataFileFolder> getDataFileFolders(DataFileFolder folder) {
		return getReferences(ODF_DATAFILEFOLDERS_REFERENCE, folder, DataFileFolder.class);
	}

	@Override
	public List<Schema> getSchemas(Database database) {
		return getReferences(ODF_SCHEMAS_REFERENCE, database, Schema.class);
	}

	@Override
	public List<Table> getTables(Schema schema) {
		return getReferences(ODF_TABLES_REFERENCE, schema, Table.class);
	}

	@Override
	public List<Column> getColumns(RelationalDataSet relationalDataSet) {
		return getReferences(ODF_COLUMNS_REFERENCE, relationalDataSet, Column.class);
	}

	@Override
	public List<Connection> getConnections(DataStore dataStore) {
		return getReferences(ODF_CONNECTIONS_REFERENCE, dataStore, Connection.class);
	}

	@Override
	public ConnectionStatus testConnection() {
		return ConnectionStatus.OK;
	}

	@Override
	public List<MetaDataObject> getChildren(MetaDataObject metaDataObject) {
		List<MetaDataObject> result = new ArrayList<MetaDataObject>();
		for (String referenceType : getReferenceTypes()) {
			for (MetaDataObject ref : getReferences(referenceType, metaDataObject, MetaDataObject.class)) {
				if (!result.contains(ref)) {
					result.add(ref);
				}
			}
		}
		return result;
	}

}
