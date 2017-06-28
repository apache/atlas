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

import org.apache.atlas.odf.api.metadata.models.Annotation;
import org.apache.atlas.odf.api.metadata.models.Column;
import org.apache.atlas.odf.api.metadata.models.DataSet;

/**
 * This class is used to cache the materialized version of a metadata reference, in order to reduce the number of retrievals required
 *
 */
public class ReferenceCache {

	private Annotation annotation;
	private Column oMColumn;
	private DataSet oMDataSet;

	public Column getColumn() {
		return oMColumn;
	}

	public void setColumn(Column oMColumn) {
		this.oMColumn = oMColumn;
	}

	public DataSet getDataSet() {
		return oMDataSet;
	}

	public void setDataSet(DataSet oMDataSet) {
		this.oMDataSet = oMDataSet;
	}

	public Annotation getAnnotation() {
		return annotation;
	}

	public void setAnnotation(Annotation annotation) {
		this.annotation = annotation;
	}

}
