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
package org.apache.atlas.odf.core.integrationtest.metadata.models;

import java.util.logging.Logger;

import org.apache.atlas.odf.core.integrationtest.metadata.MetadataStoreTestBase;
import org.apache.atlas.odf.core.metadata.WritableMetadataStore;
import org.apache.atlas.odf.core.test.ODFTestLogger;
import org.junit.Test;

import org.apache.atlas.odf.api.metadata.models.CachedMetadataStore;
import org.apache.atlas.odf.api.metadata.models.DataFile;
import org.apache.atlas.odf.api.metadata.models.DataFileFolder;
import org.apache.atlas.odf.api.metadata.models.Database;
import org.apache.atlas.odf.api.metadata.models.Table;
import org.apache.atlas.odf.core.test.TimerTestBase;

public class CachedMetadataStoreTest extends TimerTestBase {
	static protected Logger logger = ODFTestLogger.get();

	@Test
	public void testMetaDataCache() throws Exception {
		// Note that only a subset of the metadata store test cases are used here because the MetaDataCache does not support queries
		WritableMetadataStore mds = MetadataStoreTestBase.getWritableMetadataStore();
		mds.resetAllData();
		mds.createSampleData();
		MetadataStoreTestBase.createAdditionalTestData(mds);
	
		Database database = MetadataStoreTestBase.getDatabaseTestObject(mds);
		MetadataStoreTestBase.checkReferences(new CachedMetadataStore(CachedMetadataStore.retrieveMetaDataCache(mds, database)), database);

		Table table = MetadataStoreTestBase.getTableTestObject(mds);
		MetadataStoreTestBase.checkReferences(new CachedMetadataStore(CachedMetadataStore.retrieveMetaDataCache(mds, table)), table); 

		DataFileFolder folder = MetadataStoreTestBase.getDataFileFolderTestObject(mds);
		MetadataStoreTestBase.checkReferences(new CachedMetadataStore(CachedMetadataStore.retrieveMetaDataCache(mds, folder)), folder);

		DataFile file = MetadataStoreTestBase.getDataFileTestObject(mds);
		MetadataStoreTestBase.checkReferences(mds, file);
	}

}
