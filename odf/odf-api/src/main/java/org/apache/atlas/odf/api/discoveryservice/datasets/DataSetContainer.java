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

import org.apache.atlas.odf.api.metadata.models.MetaDataCache;
import org.apache.atlas.odf.api.metadata.models.MetaDataObject;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

// JSON
/**
 * This class is a reference to a metadata object in a metadata store
 * 
 */
@ApiModel(description="Container keeping reference to data set along with cached metadata objects.")
public class DataSetContainer {

	@ApiModelProperty(value="Reference to the data set to be analyzed", required=true)
	private MetaDataObject oMDataSet;

	@ApiModelProperty(value="A Metadata cache that may be used by discovery services if access to the metadata store is not available", required=false)
	private MetaDataCache metaDataCache;

	public MetaDataObject getDataSet() {
		return oMDataSet;
	}

	public void setDataSet(MetaDataObject oMDataSet) {
		this.oMDataSet = oMDataSet;
	}

	public MetaDataCache getMetaDataCache() {
		return metaDataCache;
	}

	public void setMetaDataCache(MetaDataCache metaDataCache) {
		this.metaDataCache = metaDataCache;
	}

}
