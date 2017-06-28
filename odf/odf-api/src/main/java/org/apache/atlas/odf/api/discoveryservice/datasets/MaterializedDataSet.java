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
package org.apache.atlas.odf.api.discoveryservice.datasets;

import java.util.List;

import org.apache.atlas.odf.api.metadata.models.Column;
import org.apache.atlas.odf.api.metadata.models.RelationalDataSet;

// JSON
/**
 * This class represents the materialized contents of a data set
 *
 */
public class MaterializedDataSet {
	private RelationalDataSet table;
	private List<Column> oMColumns;

	// row data in the same order as the oMColumns 
	private List<List<Object>> data;

	public List<Column> getColumns() {
		return oMColumns;
	}

	public void setColumns(List<Column> oMColumns) {
		this.oMColumns = oMColumns;
	}

	public RelationalDataSet getTable() {
		return table;
	}

	public void setTable(RelationalDataSet table) {
		this.table = table;
	}

	public List<List<Object>> getData() {
		return data;
	}

	public void setData(List<List<Object>> data) {
		this.data = data;
	}

}
