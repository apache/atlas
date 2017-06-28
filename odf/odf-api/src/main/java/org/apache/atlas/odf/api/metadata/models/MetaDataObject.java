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

// JSON
/**
 * 
 * A MetaDataObject is an object in a Metadata store, containing a reference describing its location and annotations that were created on this object
 *
 */
@ApiModel(description="Metadata object representing a generic data set.")
public abstract class MetaDataObject {

	@ApiModelProperty(value="Reference to the object (generated)", readOnly=true, required=true)
	private MetaDataObjectReference reference;

	@ApiModelProperty(value="Description of the object", required=false)
	private String description;

	@ApiModelProperty(value="Name of the object", required=true)
	private String name;

	@ApiModelProperty(value="Java class represeting the object", hidden=true)
	private String javaClass = this.getClass().getName(); // don't use JsonTypeInfo 
	
	private String originRef;
	
	private List<String> replicaRefs;

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public MetaDataObjectReference getReference() {
		return reference;
	}

	public void setReference(MetaDataObjectReference reference) {
		this.reference = reference;
	}

	public String getJavaClass() {
		return javaClass;
	}

	public void setJavaClass(String javaClass) {
		this.javaClass = javaClass;
	}

	public String getOriginRef() {
		return originRef;
	}

	public void setOriginRef(String originRef) {
		this.originRef = originRef;
	}

	public List<String> getReplicaRefs() {
		return replicaRefs;
	}

	public void setReplicaRefs(List<String> replicaRefs) {
		this.replicaRefs = replicaRefs;
	}
	
}
