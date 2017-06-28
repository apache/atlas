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

import java.util.HashMap;
import java.util.List;

import org.apache.atlas.odf.api.metadata.models.MetaDataObject;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

/**
 * Internal representation of a metadata object that is used by the @see MetaDataCache
 * In addition to the object itself this class contains all references of the object. 
 * 
 * 
 */
@ApiModel(description="Internal representation of a metadata object in the metadata cache.")
public class StoredMetaDataObject {
	@ApiModelProperty(value="Actual cached metadata object", readOnly=false, required=true)
	private MetaDataObject metaDataObject;

	@ApiModelProperty(value="Map of all references of the cached metadata object containing one reference list for each type of reference", readOnly=false, required=true)
	private HashMap<String, List<MetaDataObjectReference>> referenceMap;

	public void setMetaDataObject(MetaDataObject metaDataObject) {
		this.metaDataObject = metaDataObject;
	}

	public MetaDataObject getMetaDataObject() {
		return this.metaDataObject;
	}

	public StoredMetaDataObject() {
	}

	public StoredMetaDataObject(MetaDataObject metaDataObject) {
		this.metaDataObject = metaDataObject;
		this.referenceMap = new HashMap<String, List<MetaDataObjectReference>>();
	}

	public void setReferencesMap(HashMap<String, List<MetaDataObjectReference>> referenceMap) {
		this.referenceMap = referenceMap;
	}

	public HashMap<String, List<MetaDataObjectReference>> getReferenceMap() {
		return this.referenceMap;
	}
}
