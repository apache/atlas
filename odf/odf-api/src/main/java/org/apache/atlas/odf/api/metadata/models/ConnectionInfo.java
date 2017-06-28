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

import java.util.List;

import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

/**
 * 
 * General connecting info that must be extended for individual data sources 
 *
 */
@ApiModel(description="Object containing the information required in order to access the data behind a specific metadata object.")
public abstract class ConnectionInfo {

	@ApiModelProperty(value="Available connections for accessing the data behind the metadata object", readOnly=true, required=true)
	private List<Connection> connections;

	@ApiModelProperty(value="Reference to the actual metadata object", readOnly=true, required=true)
	private MetaDataObjectReference assetReference;

	@ApiModelProperty(value="Java class represeting the connection info object", hidden=true)
	private String javaClass = this.getClass().getName(); // don't use JsonTypeInfo 

	public List<Connection> getConnections() {
		return this.connections;
	}

	public void setConnections(List<Connection> connections) {
		this.connections = connections;
	}

	public MetaDataObjectReference getAssetReference() {
		return this.assetReference;
	}

	public void setAssetReference(MetaDataObjectReference assetReference) {
		this.assetReference = assetReference;
	}

	public String getJavaClass() {
		return javaClass;
	}

	public void setJavaClass(String javaClass) {
		this.javaClass = javaClass;
	}

}
