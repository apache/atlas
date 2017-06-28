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
package org.apache.atlas.odf.core.integrationtest.connectivity;

import java.sql.ResultSet;
import java.util.List;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.api.metadata.models.Table;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.connectivity.DataSetRetriever;
import org.apache.atlas.odf.api.connectivity.DataSetRetrieverImpl;
import org.apache.atlas.odf.api.connectivity.JDBCRetrievalResult;
import org.apache.atlas.odf.api.discoveryservice.datasets.MaterializedDataSet;
import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.core.integrationtest.metadata.importer.JDBCMetadataImporterTest;
import org.apache.atlas.odf.core.test.ODFTestBase;
import org.apache.atlas.odf.core.test.ODFTestLogger;

public class DataSetRetrieverTest extends ODFTestBase {

	static Logger logger = ODFTestLogger.get();
	
	static MetadataStore createMetadataStore() throws Exception {
		return new ODFFactory().create().getMetadataStore();
	}
	
	@BeforeClass
	public static void setupImport() throws Exception {
		MetadataStore mds = createMetadataStore();
		// create sample data only if it has not been created yet
		mds.createSampleData();
		JDBCMetadataImporterTest.runTestImport(mds);
	}
	
	@Test
	public void testDataSetRetrievalJDBC() throws Exception {
		MetadataStore ams = createMetadataStore();
		DataSetRetriever retriever = new DataSetRetrieverImpl(ams);
		List<MetaDataObjectReference> refs = ams.search(ams.newQueryBuilder().objectType("Table").build());
		Assert.assertTrue(refs.size() > 0);
		int retrievedDataSets = 0;
		for (MetaDataObjectReference ref : refs) {
			Table table = (Table) ams.retrieve(ref);
			logger.info("Retrieving table: " + table.getName() + ", " + table.getReference().getUrl());
			if (retriever.canRetrieveDataSet(table)) {
				retrievedDataSets++;
				MaterializedDataSet mds = retriever.retrieveRelationalDataSet(table);
				Assert.assertNotNull(mds);
				Assert.assertEquals(table, mds.getTable());
				int numberOfColumns = ams.getColumns(table).size();
				Assert.assertEquals(numberOfColumns, mds.getColumns().size());
				Assert.assertNotNull(mds.getData());
				Assert.assertTrue(mds.getData().size() > 0);
				for (List<Object> row : mds.getData()) {
					Assert.assertEquals(row.size(),numberOfColumns);
				}
				
				// now test JDBC method
				JDBCRetrievalResult jdbcResult = retriever.retrieveTableAsJDBCResultSet(table);
				ResultSet rs = jdbcResult.getPreparedStatement().executeQuery();
				Assert.assertEquals(mds.getColumns().size(), rs.getMetaData().getColumnCount());
				int count = 0;
				while (rs.next()) {
					count++;
				}
				Assert.assertEquals(mds.getData().size(), count);
				
				// only run one test
				break;
			}
		}
		Assert.assertEquals("Number of retrieved data sets does not meet the expected value. ", 1, retrievedDataSets);
		
	}
}
