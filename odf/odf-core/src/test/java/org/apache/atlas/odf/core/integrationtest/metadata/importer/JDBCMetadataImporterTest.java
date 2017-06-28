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
package org.apache.atlas.odf.core.integrationtest.metadata.importer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.metadata.MetadataQueryBuilder;
import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.api.metadata.importer.JDBCMetadataImportResult;
import org.apache.atlas.odf.api.metadata.importer.JDBCMetadataImporter;
import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.core.test.ODFTestBase;
import org.apache.atlas.odf.json.JSONUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.atlas.odf.api.metadata.models.JDBCConnection;
import org.apache.atlas.odf.api.metadata.models.Schema;
import org.apache.atlas.odf.api.metadata.models.Column;
import org.apache.atlas.odf.api.metadata.models.Database;
import org.apache.atlas.odf.api.metadata.models.Table;

public class JDBCMetadataImporterTest extends ODFTestBase {
	static Logger logger = Logger.getLogger(JDBCMetadataImporterTest.class.getName());

	static boolean testDBRan = false;
	public static final String SOURCE_DB1 = "DBSAMPLE1";
	public static final String SOURCE_DB2 = "DBSAMPLE2";
	public static final String DATABASE1_NAME = SOURCE_DB1;
	public static final String DATABASE2_NAME =SOURCE_DB2;
	public static final String SCHEMA1_NAME = "APP1";
	public static final String SCHEMA2_NAME = "APP2";
	public static final String TABLE1_NAME = "EMPLOYEE" + System.currentTimeMillis();
	public static final String TABLE2_NAME = "EMPLOYEE_SHORT" + System.currentTimeMillis();

	@BeforeClass
	public static void populateTestDB() throws Exception {
		if (testDBRan) {
			return;
		}
		createTestTables(SOURCE_DB1, SCHEMA1_NAME, TABLE1_NAME, TABLE2_NAME);
		createTestTables(SOURCE_DB1, SCHEMA2_NAME, TABLE1_NAME, TABLE2_NAME);
		// Switch table names so that the table named TABLE2_NAME has more columns in the SOURCE_DB2 than it has in SOURCE_DB1
		createTestTables(SOURCE_DB2, SCHEMA1_NAME, TABLE2_NAME, TABLE1_NAME);
		testDBRan = true;
	}

	private static String getConnectionUrl(String dbName) {
		String dbDir = "/tmp/odf-derby/" + dbName;
		String connectionURL = "jdbc:derby:" + dbDir + ";create=true";
		return connectionURL;
	}

	private static void createTestTables(String dbName, String schemaName, String tableName1, String tableName2) throws Exception {
		Connection conn = DriverManager.getConnection(getConnectionUrl(dbName));

		String[] stats = new String[] {
		"CREATE TABLE " + schemaName + "." + tableName1 + " (\r\n" + //
				"		EMPNO CHAR(6) NOT NULL,\r\n" + //
				"		FIRSTNME VARCHAR(12) NOT NULL,\r\n" + // 
				"		MIDINIT CHAR(1),\r\n" + //
				"		LASTNAME VARCHAR(15) NOT NULL,\r\n" + // 
				"		WORKDEPT CHAR(3),\r\n" + //
				"		PHONENO CHAR(4),\r\n" + //
				"		HIREDATE DATE,\r\n" + //
				"		JOB CHAR(8),\r\n" + //
				"		EDLEVEL SMALLINT NOT NULL,\r\n" + // 
				"		SEX CHAR(1),\r\n" + //
				"		BIRTHDATE DATE,\r\n" + //
				"		SALARY DECIMAL(9 , 2),\r\n" + // 
				"		BONUS DECIMAL(9 , 2),\r\n" + //
				"		COMM DECIMAL(9 , 2)\r\n" + //
				"	)",			
		"INSERT INTO " + schemaName + "." + tableName1 + " VALUES ('000010','CHRISTINE','I','HAAS','A00','3978','1995-01-01','PRES    ',18,'F','1963-08-24',152750.00,1000.00,4220.00)",
		"INSERT INTO " + schemaName + "." + tableName1 + " VALUES ('000020','MICHAEL','L','THOMPSON','B01','3476','2003-10-10','MANAGER ',18,'M','1978-02-02',94250.00,800.00,3300.00)",
		// Note that the 2nd table has a subset of the columns of the first table
		"CREATE TABLE " + schemaName + "." + tableName2 + " (\r\n" + //
				"		EMPNO CHAR(6) NOT NULL,\r\n" + //
				"		FIRSTNME VARCHAR(12) NOT NULL,\r\n" + //
				"		MIDINIT CHAR(1),\r\n" + //
				"		LASTNAME VARCHAR(15) NOT NULL\r\n" + //
				"	)",
		"INSERT INTO " + schemaName + "." + tableName2 + " VALUES ('000010','CHRISTINE','I','HAAS')",
		"INSERT INTO " + schemaName + "." + tableName2 + " VALUES ('000020','MICHAEL','L','THOMPSON')"
		};

		for (String stat : stats) {
			boolean result = conn.createStatement().execute(stat);
			logger.info("Result of statement: " + result);
		}
	}

	private static void runTestImport(MetadataStore mds, String connectionDbName, String importDbName, String schemaName, String tableName) throws Exception {
		populateTestDB();
		JDBCMetadataImporter importer = new ODFInternalFactory().create(JDBCMetadataImporter.class);
		JDBCConnection conn = new JDBCConnection();
		conn.setJdbcConnectionString(getConnectionUrl(connectionDbName));
		conn.setUser("dummyUser");
		conn.setPassword("dummyPassword");
		JDBCMetadataImportResult importResult = importer.importTables(conn, importDbName, schemaName, tableName);
		Assert.assertTrue("JDBCMetadataImportResult does not refer to imported database.", importResult.getDatabaseName().equals(importDbName));
		Assert.assertTrue("JDBCMetadataImportResult does not refer to imported table.", importResult.getTableNames().contains(schemaName + "." + tableName));
	}

	public static void runTestImport(MetadataStore mds) throws Exception {
		runTestImport(mds, SOURCE_DB1, DATABASE1_NAME, SCHEMA1_NAME, TABLE1_NAME);
	}

	@Test
	public void testSimpleImport() throws Exception {
		MetadataStore ams = new ODFFactory().create().getMetadataStore();
		ams.resetAllData();

		List<String> expectedDatabases = new ArrayList<String>();
		HashMap<String, List<String>> expectedSchemasForDatabase = new HashMap<String, List<String>>();
		HashMap<String, List<String>> expectedTablesForSchema = new HashMap<String, List<String>>();
		HashMap<String, List<String>> expectedColumnsForTable = new HashMap<String, List<String>>();

		runTestImport(ams, SOURCE_DB1, DATABASE1_NAME, SCHEMA1_NAME, TABLE1_NAME);

		expectedDatabases.add(DATABASE1_NAME);
		expectedSchemasForDatabase.put(DATABASE1_NAME, new ArrayList<String>());
		expectedSchemasForDatabase.get(DATABASE1_NAME).add(SCHEMA1_NAME);
		expectedTablesForSchema.put(SCHEMA1_NAME, new ArrayList<String>());
		expectedTablesForSchema.get(SCHEMA1_NAME).add(TABLE1_NAME);
		expectedColumnsForTable.put(TABLE1_NAME, new ArrayList<String>());
		expectedColumnsForTable.get(TABLE1_NAME).addAll(Arrays.asList(new String[] { "EMPNO", "FIRSTNME", "MIDINIT", "LASTNAME",
				"WORKDEPT", "PHONENO", "HIREDATE", "JOB", "EDLEVEL", "SEX", "BIRTHDATE", "SALARY", "BONUS", "COMM" }));
		validateImportedObjects(ams, expectedDatabases, expectedSchemasForDatabase, expectedTablesForSchema, expectedColumnsForTable);

		// Add another table to an existing schema in an existing database
		runTestImport(ams, SOURCE_DB1, DATABASE1_NAME, SCHEMA1_NAME, TABLE2_NAME);

		expectedTablesForSchema.get(SCHEMA1_NAME).add(TABLE2_NAME);
		expectedColumnsForTable.put(TABLE2_NAME, new ArrayList<String>());
		expectedColumnsForTable.get(TABLE2_NAME).addAll(Arrays.asList(new String[] { "EMPNO", "FIRSTNME", "MIDINIT", "LASTNAME" }));
		validateImportedObjects(ams, expectedDatabases, expectedSchemasForDatabase, expectedTablesForSchema, expectedColumnsForTable);

		// Add another schema and table to an existing database
		runTestImport(ams, SOURCE_DB1, DATABASE1_NAME, SCHEMA2_NAME, TABLE1_NAME);

		expectedSchemasForDatabase.get(DATABASE1_NAME).add(SCHEMA2_NAME);
		expectedTablesForSchema.put(SCHEMA2_NAME, new ArrayList<String>());
		expectedTablesForSchema.get(SCHEMA2_NAME).add(TABLE1_NAME);
		validateImportedObjects(ams, expectedDatabases, expectedSchemasForDatabase, expectedTablesForSchema, expectedColumnsForTable);

		// Import TABLE2_NAME again from SOURCE_DB2 where it has more columns than in SOURCE_DB1
		runTestImport(ams, SOURCE_DB2, DATABASE1_NAME, SCHEMA1_NAME, TABLE2_NAME);

		// validate that additional columns have been added to the existing table object TABLE2_NAME.
		expectedColumnsForTable.get(TABLE2_NAME).addAll(Arrays.asList(new String[] { "WORKDEPT", "PHONENO", "HIREDATE", "JOB", "EDLEVEL", "SEX", "BIRTHDATE", "SALARY", "BONUS", "COMM" }));
		validateImportedObjects(ams, expectedDatabases, expectedSchemasForDatabase, expectedTablesForSchema, expectedColumnsForTable);
	}

	private void validateImportedObjects(MetadataStore mds, List<String> expectedDatabases, HashMap<String, List<String>> expectedSchemasForDatabase, HashMap<String,
			List<String>> expectedTablesForSchema, HashMap<String, List<String>> expectedColumnsForTable) throws Exception{
		for (String dbName : expectedDatabases) {
			String query = mds.newQueryBuilder().objectType("Database").simpleCondition("name", MetadataQueryBuilder.COMPARATOR.EQUALS, dbName).build();
			List<MetaDataObjectReference> dbs = mds.search(query);
			Assert.assertEquals("Number of databases does not match expected value.", 1, dbs.size());
			Database database = (Database) mds.retrieve(dbs.get(0));
			logger.log(Level.INFO, MessageFormat.format("Reference ''{0}''.", JSONUtils.toJSON(database)));
			int numberOfMatchingConnections = 0;
			for (org.apache.atlas.odf.api.metadata.models.Connection con : mds.getConnections(database)) {
				if (getConnectionUrl(database.getName()).equals(((JDBCConnection) mds.retrieve(con.getReference())).getJdbcConnectionString())) {
					numberOfMatchingConnections++;
				}
			}
			Assert.assertEquals("Number of matching JDBC connections does not match expected value.", 1, numberOfMatchingConnections);
			List<String> actualSchemaNames = new ArrayList<String>();
			for (Schema schema : mds.getSchemas(database)) {
				actualSchemaNames.add(schema.getName());

				List<String> actualTableNames = new ArrayList<String>();
				for (Table table : mds.getTables(schema)) {
					actualTableNames.add(table.getName());

					List<String> actualColumnNames = new ArrayList<String>();
					for (Column column : mds.getColumns(table)) {
						actualColumnNames.add(column.getName());
					}
					Assert.assertTrue("Expected columns are missing from metadata store.", actualColumnNames.containsAll(expectedColumnsForTable.get(table.getName())));
					Assert.assertTrue("Importer has not imported all expected columns.", expectedColumnsForTable.get(table.getName()).containsAll(actualColumnNames));
				}
				Assert.assertTrue("Expected tables are missing from metadata store.", actualTableNames.containsAll(expectedTablesForSchema.get(schema.getName())));
				Assert.assertTrue("Importer has not imported all expected tables.", expectedTablesForSchema.get(schema.getName()).containsAll(actualTableNames));
			}
			Assert.assertTrue("Expected schemas are missing from metadata store.", actualSchemaNames.containsAll(expectedSchemasForDatabase.get(database.getName())));
			Assert.assertTrue("Importer has not imported all expected schemas.", expectedSchemasForDatabase.get(database.getName()).containsAll(actualSchemaNames));
		}
	}
}
