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
package org.apache.atlas.odf.api.connectivity;

import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.api.metadata.models.DataSet;
import org.apache.atlas.odf.api.metadata.models.RelationalDataSet;
import org.apache.atlas.odf.api.metadata.models.Table;
import org.apache.atlas.odf.api.discoveryservice.datasets.MaterializedDataSet;

public interface DataSetRetriever {

	void setMetadataStore(MetadataStore mds);

	boolean canRetrieveDataSet(DataSet oMDataSet);
	
	MaterializedDataSet retrieveRelationalDataSet(RelationalDataSet relationalDataSet) throws DataRetrievalException;
	
	void createCsvFile(RelationalDataSet relationalDataSet, String fileName) throws DataRetrievalException;

	JDBCRetrievalResult retrieveTableAsJDBCResultSet(Table oMTable) throws DataRetrievalException;
}
