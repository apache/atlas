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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.metadata.MetadataQueryBuilder;
import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.api.metadata.importer.JDBCMetadataImportResult;
import org.apache.atlas.odf.api.metadata.importer.JDBCMetadataImporter;
import org.apache.atlas.odf.api.metadata.importer.MetadataImportException;
import org.apache.atlas.odf.api.metadata.models.JDBCConnection;
import org.apache.atlas.odf.api.metadata.models.MetaDataObject;
import org.apache.atlas.odf.api.metadata.models.Column;
import org.apache.atlas.odf.api.metadata.models.Database;
import org.apache.atlas.odf.api.metadata.models.Schema;
import org.apache.atlas.odf.api.metadata.models.Table;

public class JDBCMetadataImporterImpl implements JDBCMetadataImporter {
	Logger logger = Logger.getLogger(JDBCMetadataImporterImpl.class.getName());
	private WritableMetadataStore mds;
	WritableMetadataStoreUtils mdsUtils;

	public JDBCMetadataImporterImpl() {
		MetadataStore currentMds = new ODFFactory().create().getMetadataStore();
		if (currentMds instanceof WritableMetadataStore) {
			this.mds = (WritableMetadataStore) currentMds;
		} else {
			String errorText = "Cannot import data because metadata store ''{0}'' does not support the WritableMetadataStore interface.";
			throw new RuntimeException(MessageFormat.format(errorText , currentMds.getClass()));
		}
	}

	@Override
	public JDBCMetadataImportResult importTables(JDBCConnection connection, String dbName, String schemaPattern, String tableNamePattern)  {
		Connection conn = null;
		try {
			logger.log(Level.FINE, "Importing tables...");
			conn = DriverManager.getConnection(connection.getJdbcConnectionString(), connection.getUser(), connection.getPassword());
			DatabaseMetaData dmd = conn.getMetaData();
			List<MetaDataObjectReference> matchingDatabases = mds.search(mds.newQueryBuilder().objectType("Database").simpleCondition("name", MetadataQueryBuilder.COMPARATOR.EQUALS, dbName).build());
			Database odfDatabase = null;
			if (!matchingDatabases.isEmpty()) {
				odfDatabase = (Database) mds.retrieve(matchingDatabases.get(0));
				mds.updateObject(odfDatabase);
			} else {
				odfDatabase = new Database();
				List<MetaDataObjectReference> conList = new ArrayList<MetaDataObjectReference>();
				odfDatabase.setConnections(conList);
				odfDatabase.setName(dbName);
				odfDatabase.setDbType(dmd.getDatabaseProductName());
				odfDatabase.setDescription("Database " + dbName + " imported by JDBC2AtlasImporter on " + new Date());
				mds.createObject(odfDatabase);
			}
			Map<String, Table> tableMap = new HashMap<String, Table>();
			Map<String, Schema> schemaMap = new HashMap<>();
			List<MetaDataObjectReference> schemaList = new ArrayList<MetaDataObjectReference>();
			Set<String> tableNames = new HashSet<>();
			ResultSet columnRS = dmd.getColumns(null, schemaPattern, tableNamePattern, null);
			while (columnRS.next()) {
				String columnName = columnRS.getString("COLUMN_NAME");
				String schemaName = columnRS.getString("TABLE_SCHEM");
				String tableName = columnRS.getString("TABLE_NAME");
				String dataType = columnRS.getString("TYPE_NAME");
				
				Schema schema = schemaMap.get(schemaName);
				if (schema == null) {
					for (Schema s : mds.getSchemas(odfDatabase)) {
						if (schemaName.equals(s.getName())) {
							schema = s;
							mds.updateObject(schema);
							break;
						}
					}
					if (schema == null) {
						schema = new Schema();
						schema.setName(schemaName);
						schemaList.add(mds.createObject(schema));
					}
					schemaMap.put(schemaName, schema);
					mds.addSchemaReference(odfDatabase, schema.getReference());
				}
				
				String key = schemaName + "." + tableName;
				Table tableObject = tableMap.get(key);
				if (tableObject == null) {
					for (Table t : mds.getTables(schema)) {
						if (tableName.equals(t.getName())) {
							tableObject = t;
							mds.updateObject(tableObject);
							break;
						}
					}
					if (tableObject == null) {
						tableObject = new Table();
						tableObject.setName(tableName);
						MetaDataObjectReference ref = mds.createObject(tableObject);
						tableObject.setReference(ref);
					}
					tableNames.add(tableName);
					tableMap.put(key, tableObject);
					mds.addTableReference(schema, tableObject.getReference());
				}
				Column column = null;
				for (Column c : mds.getColumns(tableObject)) {
					if (columnName.equals(c.getName())) {
						column = c;
						break;
					}
				}
				if (column == null) {
					// Add new column only if a column with the same name does not exist
					column = WritableMetadataStoreUtils.createColumn(columnName, dataType, null);
					mds.createObject(column);
				}
				mds.addColumnReference(tableObject, column.getReference());
			}
			columnRS.close();
			logger.log(Level.INFO, "Found {0} tables in database ''{1}'': ''{2}''", new Object[]{tableMap.keySet().size(), dbName, tableNames });

			JDBCConnection odfConnection = null;
			for (MetaDataObject c : mds.getConnections(odfDatabase)) {
				if ((c instanceof JDBCConnection) && connection.getJdbcConnectionString().equals(((JDBCConnection) c).getJdbcConnectionString())) {
					odfConnection = (JDBCConnection) c;
					mds.updateObject(odfConnection);
					break;
				}
			}
			if (odfConnection == null) {
				odfConnection = new JDBCConnection();
				odfConnection.setJdbcConnectionString(connection.getJdbcConnectionString());
				odfConnection.setUser(connection.getUser());
				odfConnection.setPassword(connection.getPassword());
				odfConnection.setDescription("JDBC connection for database " + dbName);
				mds.createObject(odfConnection);
			}
			mds.addConnectionReference(odfDatabase, odfConnection.getReference());

			mds.commit();
			return new JDBCMetadataImportResult(dbName, odfDatabase.getReference().getId(), new ArrayList<String>( tableMap.keySet() ));
		} catch (SQLException exc) {
			throw new MetadataImportException(exc);
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}

	}
}
