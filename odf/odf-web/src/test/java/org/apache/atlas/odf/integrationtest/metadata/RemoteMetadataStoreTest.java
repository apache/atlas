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
package org.apache.atlas.odf.integrationtest.metadata;

import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.atlas.odf.core.Encryption;
import org.apache.atlas.odf.core.integrationtest.metadata.MetadataStoreTestBase;
import org.apache.atlas.odf.rest.test.RestTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.metadata.MetadataQueryBuilder;
import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.api.metadata.MetadataStoreException;
import org.apache.atlas.odf.api.metadata.RemoteMetadataStore;

public class RemoteMetadataStoreTest extends MetadataStoreTestBase {

	protected MetadataStore getMetadataStore() {
		RemoteMetadataStore rms = null;
		try {
			rms = new RemoteMetadataStore(RestTestBase.getOdfBaseUrl(), RestTestBase.getOdfUser(), Encryption.decryptText(RestTestBase.getOdfPassword()), true);
		} catch (MetadataStoreException | URISyntaxException e) {
			throw new RuntimeException("Error connecting to remote metadata store,", e);
		}
		return rms;
	}

	//TODO: Remove all methods below this comment once the DefaultMetadataStore is queue-based (issue #122)
	// RemoteMetadataStore will then use the exact same test cases as the other (writable) metadata stores 

	@Before
	public void createSampleData() {
		//TODO: Remove this method once the DefaultMetadataStore is queue-based (issue #122)
		MetadataStore mds = getMetadataStore();
		mds.resetAllData();
		mds.createSampleData();
	}

	@Test
	public void testProperties() throws Exception {
		//TODO: Remove this method once the DefaultMetadataStore is queue-based (issue #122)
		RemoteMetadataStore rms = new RemoteMetadataStore(RestTestBase.getOdfBaseUrl(), RestTestBase.getOdfUser(), Encryption.decryptText(RestTestBase.getOdfPassword()), true);
		Properties props = rms.getProperties();
		Assert.assertNotNull(props);
		Assert.assertTrue(!props.isEmpty());
	}

	@Test
	public void testReferences() throws Exception {
		//TODO: Do not overwrite original method once DefaultMetadataStore is queue-based
		MetadataStore mds = getMetadataStore();
		MetadataStoreTestBase.checkReferences(mds, MetadataStoreTestBase.getDataFileTestObject(mds));
	}

	@Test
	public void testSearchAndRetrieve() {
		//TODO: Do not overwrite original method once DefaultMetadataStore is queue-based

		// Test retrieve
		MetadataStore mds = getMetadataStore();
		MetaDataObjectReference bankClientsShortRef = mds.search(mds.newQueryBuilder().objectType("DataFile").simpleCondition("name", MetadataQueryBuilder.COMPARATOR.EQUALS, "BankClientsShort").build()).get(0);
		Assert.assertEquals("The metadata store did not retrieve the object with the expected name.", "BankClientsShort", mds.retrieve(bankClientsShortRef).getName());

		// Test queries with conditions
		checkQueryResults(mds, new String[] { "BankClientsShort" }, mds.newQueryBuilder().objectType("DataFile").simpleCondition("name", MetadataQueryBuilder.COMPARATOR.EQUALS, "BankClientsShort").build(), false);
		checkQueryResults(mds, new String[] { "SimpleExampleTable" }, mds.newQueryBuilder().objectType("DataFile").simpleCondition("name", MetadataQueryBuilder.COMPARATOR.NOT_EQUALS, "BankClientsShort").build(), false);
		checkQueryResults(mds, new String[] { "NAME" },
				mds.newQueryBuilder().objectType("Column").simpleCondition("name", MetadataQueryBuilder.COMPARATOR.EQUALS, "NAME").simpleCondition("dataType", MetadataQueryBuilder.COMPARATOR.EQUALS, "string").build(), false);

		// Test type hierarchy
		checkQueryResults(mds, new String[] { "BankClientsShort", "SimpleExampleTable" }, mds.newQueryBuilder().objectType("DataFile").build(), true);
		checkQueryResults(mds, new String[] { "BankClientsShort", "SimpleExampleTable" }, mds.newQueryBuilder().objectType("RelationalDataSet").build(), true);
		checkQueryResults(mds, new String[] { "BankClientsShort", "SimpleExampleTable", "Simple URL example document", "Simple local example document" }, mds.newQueryBuilder().objectType("DataSet").build(), false);
		checkQueryResults(mds, new String[] { "BankClientsShort" }, mds.newQueryBuilder().objectType("MetaDataObject").simpleCondition("name", MetadataQueryBuilder.COMPARATOR.EQUALS, "BankClientsShort").build(), false);
	}

	@Test
	public void testAnnotations() {
		//TODO: Remove this method once the DefaultMetadataStore is queue-based (issue #122)
	}
}
