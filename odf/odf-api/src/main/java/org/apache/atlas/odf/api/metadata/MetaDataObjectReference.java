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

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

// JSON
/**
 * This class describes the location of a MetadataObject
 *
 */
@ApiModel(description="Reference to a metadata object.")
public class MetaDataObjectReference {
	@ApiModelProperty(value="Unique id of the object", required=true)
	private String id;

	@ApiModelProperty(value="Id of the metadata repository where the object is registered", required=true)
	private String repositoryId;

	@ApiModelProperty(value="URL of the object in the metadata repository", required=true)
	private String url;

	@JsonIgnore
	private ReferenceCache cache;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public boolean equals(Object other) {
		if (other == null) {
			return false;
		}
		if (!(other instanceof MetaDataObjectReference)) {
			return false;
		}
		MetaDataObjectReference otherMDO = (MetaDataObjectReference) other;
		if (!this.id.equals(otherMDO.id)) {
			return false;
		}
		if (this.repositoryId == null) {
			return otherMDO.repositoryId == null;
		}
		return this.repositoryId.equals(otherMDO.repositoryId);
	}

	public int hashCode() {
		int result = 0;
		if (this.repositoryId != null) {
			result = repositoryId.hashCode();
		}
		return result + this.id.hashCode();
	}

	public String toString() {
		return this.repositoryId + "|||" + this.id;
	}

	public String getRepositoryId() {
		return repositoryId;
	}

	public void setRepositoryId(String repositoryId) {
		this.repositoryId = repositoryId;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public ReferenceCache getCache() {
		return cache;
	}

	public void setCache(ReferenceCache cache) {
		this.cache = cache;
	}

}
