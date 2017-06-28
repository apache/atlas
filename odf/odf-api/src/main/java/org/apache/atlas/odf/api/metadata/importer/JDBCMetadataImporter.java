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
package org.apache.atlas.odf.api.metadata.importer;

import org.apache.atlas.odf.api.metadata.models.JDBCConnection;

/**
 * Interface of the utility that imports metadata from JDBC data sources into the ODF metadata store.
 * 
 */
public interface JDBCMetadataImporter {
	
	/**
	 * Import metadata of one or multiple relational tables into the ODF metadata store, along with the corresponding
	 * database and connection information.
	 * 
	 * @param connection Connection to the JDBC data soure
	 * @param dbName Database name
	 * @param schemaPattern Database schema name or pattern
	 * @param tableNamePattern Table name or pattern
	 * @return Object containing the raw results of the import operation
	 */
	public JDBCMetadataImportResult importTables(JDBCConnection connection, String dbName, String schemaPattern, String tableNamePattern);

}
