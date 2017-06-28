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
package org.apache.atlas.odf.api.metadata.models;

import java.util.ArrayList;
import java.util.List;

import org.apache.atlas.odf.api.metadata.StoredMetaDataObject;

import io.swagger.annotations.ApiModelProperty;

public class MetaDataCache {

	@ApiModelProperty(value="Cached metadata objects to be used by discovery services if access to the metadata store is not available", required=false)
	private List<StoredMetaDataObject> metaDataObjects = new ArrayList<StoredMetaDataObject>();

	@ApiModelProperty(value="Cached connection info objects to be used by discovery services if access to the metadata store is not available", required=false)
	private List<ConnectionInfo> connectionInfoObjects = new ArrayList<ConnectionInfo>();

	public List<StoredMetaDataObject> getMetaDataObjects() {
		return metaDataObjects;
	}

	public void setMetaDataObjects(List<StoredMetaDataObject> metaDataObjects) {
		this.metaDataObjects = metaDataObjects;
	}

	public List<ConnectionInfo> getConnectionInfoObjects() {
		return this.connectionInfoObjects;
	}

	public void setConnectionInfoObjects(List<ConnectionInfo> connectionInfoObjects) {
		this.connectionInfoObjects = connectionInfoObjects;
	}

}
