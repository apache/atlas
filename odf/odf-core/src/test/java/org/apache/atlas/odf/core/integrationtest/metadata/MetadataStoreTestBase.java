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
package org.apache.atlas.odf.core.integrationtest.metadata;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.core.metadata.WritableMetadataStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.metadata.DefaultMetadataQueryBuilder;
import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.metadata.MetadataQueryBuilder;
import org.apache.atlas.odf.api.metadata.models.Schema;
import org.apache.atlas.odf.api.metadata.models.Table;
import org.apache.atlas.odf.api.metadata.models.ProfilingAnnotation;
import org.apache.atlas.odf.api.metadata.models.RelationshipAnnotation;
import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.api.metadata.MetadataStoreException;
import org.apache.atlas.odf.api.metadata.models.ClassificationAnnotation;
import org.apache.atlas.odf.api.metadata.models.Column;
import org.apache.atlas.odf.api.metadata.models.Connection;
import org.apache.atlas.odf.api.metadata.models.DataFile;
import org.apache.atlas.odf.api.metadata.models.DataFileFolder;
import org.apache.atlas.odf.api.metadata.models.JDBCConnection;
import org.apache.atlas.odf.api.metadata.models.JDBCConnectionInfo;
import org.apache.atlas.odf.api.metadata.models.MetaDataObject;
import org.apache.atlas.odf.api.metadata.models.Database;

public abstract class MetadataStoreTestBase {
	private Logger logger = Logger.getLogger(MetadataStoreTestBase.class.getName());
	private static final String analysisRun = UUID.randomUUID().toString();

	protected abstract MetadataStore getMetadataStore();

	public static WritableMetadataStore getWritableMetadataStore() {
		MetadataStore mds = new ODFFactory().create().getMetadataStore();
		if (!(mds instanceof WritableMetadataStore)) {
			String errorText = "The MetadataStore implementation ''{0}'' does not support the WritableMetadataStore interface.";
			Assert.fail(MessageFormat.format(errorText , mds.getClass()));
			return null;
		}
		return (WritableMetadataStore) mds;
	}

	public static void createAdditionalTestData(WritableMetadataStore mds) {
		MetaDataObjectReference bankClientsShortRef = mds.search(mds.newQueryBuilder().objectType("DataFile").simpleCondition("name", MetadataQueryBuilder.COMPARATOR.EQUALS, "BankClientsShort").build()).get(0);

		JDBCConnection connection = new JDBCConnection();
		connection.setName("connection1");

		Table table1 = new Table();
		table1.setName("table1");
		Table table2 = new Table();
		table2.setName("table2");

		Schema schema1 = new Schema();
		schema1.setName("schema1");
		MetaDataObjectReference schemaRef = mds.createObject(schema1);
		mds.addTableReference(schema1, mds.createObject(table1));
		mds.addTableReference(schema1, mds.createObject(table2));

		Database dataStore = new Database();
		dataStore.setName("database1");
		mds.createObject(dataStore);
		mds.addSchemaReference(dataStore, schemaRef);
		mds.addConnectionReference(dataStore, mds.createObject(connection));

		DataFile file1 = new DataFile();
		file1.setName("file1");
		DataFile file2 = new DataFile();
		file2.setName("file2");

		DataFileFolder nestedFolder = new DataFileFolder();
		nestedFolder.setName("nestedFolder");
		MetaDataObjectReference nestedFolderRef = mds.createObject(nestedFolder);
		mds.addDataFileReference(nestedFolder, mds.createObject(file1));
		mds.addDataFileReference(nestedFolder, mds.createObject(file2));

		DataFileFolder rootFolder = new DataFileFolder();
		rootFolder.setName("rootFolder");
		mds.createObject(rootFolder);
		mds.addDataFileFolderReference(rootFolder, nestedFolderRef);

		ProfilingAnnotation pa = new ProfilingAnnotation();
		pa.setName("A profiling annotation");
		pa.setProfiledObject(bankClientsShortRef);
		pa.setAnalysisRun(analysisRun);
		mds.createObject(pa);

		ClassificationAnnotation ca = new ClassificationAnnotation();
		ca.setName("A classification annotation");
		ca.setClassifiedObject(bankClientsShortRef);
		ca.setAnalysisRun(analysisRun);
		ca.setClassifyingObjects(Collections.singletonList(bankClientsShortRef));
		mds.createObject(ca);

		RelationshipAnnotation ra = new RelationshipAnnotation();
		ra.setName("A relationship annotation");
		ra.setRelatedObjects(Collections.singletonList(bankClientsShortRef));
		ra.setAnalysisRun(analysisRun);
		mds.createObject(ra);

		mds.commit();
	}

	@Before
	public void createSampleData() {
		WritableMetadataStore mds = getWritableMetadataStore();
		mds.resetAllData();
		mds.createSampleData();
		createAdditionalTestData(mds);
	}

	public static void checkQueryResults(MetadataStore mds, String[] expectedObjectNames, String searchTerm, boolean isSubset) {
		HashSet<String> expectedResults = new HashSet<String>(Arrays.asList(expectedObjectNames));
		List<MetaDataObjectReference> searchResult = mds.search(searchTerm);
		Set<String> foundResults = new HashSet<>();
		for (MetaDataObjectReference ref : searchResult) {
			foundResults.add(mds.retrieve(ref).getName());
		}
		if (isSubset) {
			String messageText = "Metadata search term ''{0}'' did not return expected subset of objects. Expected ''{1}'' but received ''{2}''.";
			Assert.assertTrue(MessageFormat.format(messageText, new Object[] {searchTerm, expectedResults, foundResults}), foundResults.containsAll(expectedResults));
		} else {
			String messageText = "Metadata search term ''{0}'' did not return expected results. Expected ''{1}'' but received ''{2}''.";
			Assert.assertTrue(MessageFormat.format(messageText, new Object[] {searchTerm, expectedResults, foundResults}), foundResults.equals(expectedResults));
		}
	}

	public static void checkReferencedObjects(String[] expectedObjectNames, List<? extends MetaDataObject> referencedObjects, boolean isSubset) {
		HashSet<String> expectedResults = new HashSet<String>(Arrays.asList(expectedObjectNames));
		Set<String> actualNames = new HashSet<>();
		for (MetaDataObject obj : referencedObjects) {
			actualNames.add(obj.getName());
		}
		if (isSubset) {
			String messageText = "Actual object names ''{0}'' are not a subset of expected names ''{1}''.";
			Assert.assertTrue(MessageFormat.format(messageText, new Object[] { actualNames, expectedResults }), actualNames.containsAll(expectedResults));
		} else {
			String messageText = "Actual object names ''{0}'' do not match expected names ''{1}''.";
			Assert.assertTrue(MessageFormat.format(messageText, new Object[] { actualNames, expectedResults }), actualNames.equals(expectedResults));
		}
	}

	void checkFailingQuery(MetadataStore mds, String searchTerm) {
		try {
			logger.log(Level.INFO, "Checking incorrect query \"{0}\"", searchTerm);
			List<MetaDataObjectReference> searchResult = mds.search(searchTerm);
			if (searchResult != null) {
				// Search must return null or throw exception
				Assert.fail(MessageFormat.format("Incorrect query \"{0}\" did not throw the expected exception.", searchTerm));
			}
		} catch (MetadataStoreException e) {
			logger.log(Level.INFO, "Catching expected exception.", e);
		}
	}

	@Test
	public void testSearchAndRetrieve() {
		MetadataStore mds = getMetadataStore();
		MetaDataObjectReference bankClientsShortRef = mds.search(mds.newQueryBuilder().objectType("DataFile").simpleCondition("name", MetadataQueryBuilder.COMPARATOR.EQUALS, "BankClientsShort").build()).get(0);
		Assert.assertEquals("The metadata store did not retrieve the object with the expected name.", "BankClientsShort", mds.retrieve(bankClientsShortRef).getName());

		// Test queries with conditions
		checkQueryResults(mds, new String[] { "BankClientsShort" }, mds.newQueryBuilder().objectType("DataFile").simpleCondition("name", MetadataQueryBuilder.COMPARATOR.EQUALS, "BankClientsShort").build(), false);
		checkQueryResults(mds, new String[] { "SimpleExampleTable", "file2", "file1"}, mds.newQueryBuilder().objectType("DataFile").simpleCondition("name", MetadataQueryBuilder.COMPARATOR.NOT_EQUALS, "BankClientsShort").build(), false);
		checkQueryResults(mds, new String[] { "NAME" },
				mds.newQueryBuilder().objectType("Column").simpleCondition("name", MetadataQueryBuilder.COMPARATOR.EQUALS, "NAME").simpleCondition("dataType", MetadataQueryBuilder.COMPARATOR.EQUALS, "string").build(), false);

		// Test type hierarchy
		checkQueryResults(mds, new String[] { "BankClientsShort", "SimpleExampleTable" }, mds.newQueryBuilder().objectType("DataFile").build(), true);
		checkQueryResults(mds, new String[] { "BankClientsShort", "SimpleExampleTable" }, mds.newQueryBuilder().objectType("RelationalDataSet").build(), true);
		checkQueryResults(mds, new String[] { "BankClientsShort", "SimpleExampleTable", "Simple URL example document", "Simple local example document", "table1", "table2", "file2", "file1" }, mds.newQueryBuilder().objectType("DataSet").build(), false);
		checkQueryResults(mds, new String[] { "BankClientsShort" }, mds.newQueryBuilder().objectType("MetaDataObject").simpleCondition("name", MetadataQueryBuilder.COMPARATOR.EQUALS, "BankClientsShort").build(), false);
	}
	
	public static Database getDatabaseTestObject(MetadataStore mds) {
		String dataStoreQuery = mds.newQueryBuilder().objectType("DataStore").build();
		MetadataStoreTestBase.checkQueryResults(mds, new String[] { "database1"}, dataStoreQuery, false);
		return (Database) mds.retrieve(mds.search(dataStoreQuery).get(0));
	}

	public static Table getTableTestObject(MetadataStore mds) {
		String tableQuery = mds.newQueryBuilder().objectType("Table").simpleCondition("name", MetadataQueryBuilder.COMPARATOR.EQUALS, "table1").build();
		MetadataStoreTestBase.checkQueryResults(mds, new String[] { "table1"}, tableQuery, false);
		return (Table) mds.retrieve(mds.search(tableQuery).get(0));
	}

	public static DataFile getDataFileTestObject(MetadataStore mds) {
		String dataFileQuery = mds.newQueryBuilder().objectType("DataFile").simpleCondition("name", MetadataQueryBuilder.COMPARATOR.EQUALS, "SimpleExampleTable").build();
		MetadataStoreTestBase.checkQueryResults(mds, new String[] { "SimpleExampleTable"}, dataFileQuery, false);
		return (DataFile) mds.retrieve(mds.search(dataFileQuery).get(0));
	}

	public static DataFileFolder getDataFileFolderTestObject(MetadataStore mds) {
		String folderQuery = mds.newQueryBuilder().objectType("DataFileFolder").simpleCondition("name", MetadataQueryBuilder.COMPARATOR.EQUALS, "rootFolder").build();
		MetadataStoreTestBase.checkQueryResults(mds, new String[] { "rootFolder"}, folderQuery, false);
		return (DataFileFolder) mds.retrieve(mds.search(folderQuery).get(0));
	}

	public static void checkReferences(MetadataStore mds, Database database) throws Exception {
		List<Schema> schemaList = mds.getSchemas(database);
		MetadataStoreTestBase.checkReferencedObjects(new String[] { "schema1" }, schemaList, false);
		List<Table> tableList = mds.getTables(schemaList.get(0));
		MetadataStoreTestBase.checkReferencedObjects(new String[] { "table1", "table2" }, tableList, false);
		List<Connection> connectionList = mds.getConnections(database);
		MetadataStoreTestBase.checkReferencedObjects(new String[] { "connection1" }, connectionList, false);
	}

	public static void checkReferences(MetadataStore mds, Table table) throws Exception {
		JDBCConnectionInfo connectionInfo = (JDBCConnectionInfo) mds.getConnectionInfo(table);
		Assert.assertTrue("Connection is not set in connection info.", connectionInfo.getConnections().size() > 0);
		Assert.assertEquals("Connection does not match expected name.", "connection1", connectionInfo.getConnections().get(0).getName());
		Assert.assertEquals("Schema name of connection info does not match expected value.", "schema1", connectionInfo.getSchemaName());
	}

	public static void checkReferences(MetadataStore mds, DataFileFolder folder) throws Exception {
		List<DataFileFolder> nestedFolderList = mds.getDataFileFolders(folder);
		MetadataStoreTestBase.checkReferencedObjects(new String[] { "nestedFolder" }, nestedFolderList, false);
		List<DataFile> fileList = mds.getDataFiles(nestedFolderList.get(0));
		MetadataStoreTestBase.checkReferencedObjects(new String[] { "file1", "file2" }, fileList, false);
	}

	public static void checkReferences(MetadataStore mds, DataFile file) throws Exception {
		List<Column> columnList = mds.getColumns(file);
		MetadataStoreTestBase.checkReferencedObjects(new String[] { "ColumnName1", "ColumnName2" }, columnList, false);
		MetadataStoreTestBase.checkReferencedObjects(new String[] { "SimpleExampleTable" }, Collections.singletonList(mds.getParent(columnList.get(0))), false);
		MetadataStoreTestBase.checkReferencedObjects(new String[] { "ColumnName1", "ColumnName2" }, mds.getChildren(file), false);
	}

	@Test
	public void testReferences() throws Exception {
		MetadataStore mds = getMetadataStore();
		checkReferences(mds, getDatabaseTestObject(mds));
		checkReferences(mds, getTableTestObject(mds));
		checkReferences(mds, getDataFileFolderTestObject(mds));
		checkReferences(mds, getDataFileTestObject(mds));
	}

	@Test
	public void testErrorHandling() {
		MetadataStore mds = getMetadataStore();
		MetaDataObjectReference nonExistentRef = new MetaDataObjectReference();
		nonExistentRef.setId("non-existing-reference-id");
		nonExistentRef.setRepositoryId(mds.getRepositoryId());

		Assert.assertEquals("A null value was expected when retrieving a non-existend object.", null, mds.retrieve(nonExistentRef));
		String errorText = "Metadata search should have returned an empty result set.";
		Assert.assertEquals(errorText,  mds.search(mds.newQueryBuilder().objectType("nonExistentType").build()), new ArrayList<MetaDataObjectReference>());
		Assert.assertEquals(errorText,  mds.search(mds.newQueryBuilder().objectType("DataFile").simpleCondition("name", MetadataQueryBuilder.COMPARATOR.EQUALS, "nonExistentName").build()), new ArrayList<MetaDataObjectReference>());

		if (!mds.getProperties().get(MetadataStore.STORE_PROPERTY_TYPE).equals("atlas")) {
			// Skip this test because Atlas accepts this query as text search
			checkFailingQuery(mds, "justAsSingleToken");
			// Skip this test of Atlas because it does not return an error
			String validQueryWithCondition = mds.newQueryBuilder().objectType("DataFile").simpleCondition("name", MetadataQueryBuilder.COMPARATOR.EQUALS, "BankClientsShort").build();
			checkFailingQuery(mds, validQueryWithCondition + DefaultMetadataQueryBuilder.SEPARATOR_STRING + "additionalTrailingToken");
			String validDataSetQuery = mds.newQueryBuilder().objectType("DataFile").build();
			checkFailingQuery(mds, validDataSetQuery + DefaultMetadataQueryBuilder.SEPARATOR_STRING + "additionalTrailingToken");
		}
	}

	@Test
	public void testAnnotations() {
		MetadataStore mds = getMetadataStore();

		String annotationQueryString = mds.newQueryBuilder().objectType("Annotation").build();
		checkQueryResults(mds, new String[] { "A profiling annotation", "A classification annotation", "A relationship annotation" }, annotationQueryString, false);
		String analysisRunQuery = mds.newQueryBuilder().objectType("Annotation").simpleCondition("analysisRun", MetadataQueryBuilder.COMPARATOR.EQUALS, analysisRun).build();
		checkQueryResults(mds, new String[] { "A profiling annotation", "A classification annotation", "A relationship annotation" }, analysisRunQuery, false);
	}

	@Test
	public void testResetAllData() {
		MetadataStore mds = getMetadataStore();
		mds.resetAllData();
		String emptyResultSet = mds.newQueryBuilder().objectType("MetaDataObject").build();
		checkQueryResults(mds, new String[] {}, emptyResultSet, false);
	}
}
