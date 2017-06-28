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
package org.apache.atlas.odf.api.discoveryservice;

import java.util.List;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

// JSON
/**
 * 
 * This class is used for the registration of a service at an ODF instance.
 * A JSON document of this type must be provided by a remote discovery service implementation in order to register it with an ODF instance
 *
 */
@ApiModel(description="Parameters describing a discovery service.")
public class DiscoveryServiceProperties {
	@ApiModelProperty(value="Unique id string of the discovery service", required=true)
	private String id;

	@ApiModelProperty(value="Descriptive name of the discovery service", required=true)
	private String name;

	@ApiModelProperty(value="Optional description of the discovery service")
	private String description;

	@ApiModelProperty(value="Optional custom description of the discovery service")
	private String customDescription;

	@ApiModelProperty(value="Optional link to a JPG or PNG image illustrating the discovery service")
	private String iconUrl;

	@ApiModelProperty(value="Optional URL pointing to the description of the discovery service")
	private String link;
	
	public String getCustomDescription() {
		return customDescription;
	}

	public void setCustomDescription(String customDescription) {
		this.customDescription = customDescription;
	}

	public List<String> getPrerequisiteAnnotationTypes() {
		return prerequisiteAnnotationTypes;
	}

	public void setPrerequisiteAnnotationTypes(List<String> prerequisiteAnnotationTypes) {
		this.prerequisiteAnnotationTypes = prerequisiteAnnotationTypes;
	}

	public List<String> getResultingAnnotationTypes() {
		return resultingAnnotationTypes;
	}

	public void setResultingAnnotationTypes(List<String> resultingAnnotationTypes) {
		this.resultingAnnotationTypes = resultingAnnotationTypes;
	}

	public List<String> getSupportedObjectTypes() {
		return supportedObjectTypes;
	}

	public void setSupportedObjectTypes(List<String> supportedObjectTypes) {
		this.supportedObjectTypes = supportedObjectTypes;
	}

	public List<String> getAssignedObjectTypes() {
		return assignedObjectTypes;
	}

	public void setAssignedObjectTypes(List<String> assignedObjectTypes) {
		this.assignedObjectTypes = assignedObjectTypes;
	}

	public List<String> getAssignedObjectCandidates() {
		return assignedObjectCandidates;
	}

	public void setAssignedObjectCandidates(List<String> assignedObjectCandidates) {
		this.assignedObjectCandidates = assignedObjectCandidates;
	}

	@ApiModelProperty(value="List of prerequisite annotation types required to run the discovery service")
	private List<String> prerequisiteAnnotationTypes;

	@ApiModelProperty(value="List annotation types created by the discovery service")
	private List<String> resultingAnnotationTypes;

	@ApiModelProperty(value="Types of objects that can be analyzed by the discovery service")
	private List<String> supportedObjectTypes;

	@ApiModelProperty(value="Types of objects that may be assigned to the resulting annotations")
	private List<String> assignedObjectTypes;

	@ApiModelProperty(value="Ids of specific objects (e.g. data classes) that may be assigned to resulting annotations")
	private List<String> assignedObjectCandidates;

	@ApiModelProperty(value = "Number of parallel analyses the service can handle, with a default of 2")
	private Integer parallelismCount = 2;

	@ApiModelProperty(value="Endpoint of the discovery service", required=true)
	private DiscoveryServiceEndpoint endpoint;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getIconUrl() {
		return iconUrl;
	}

	public void setIconUrl(String iconURL) {
		this.iconUrl = iconURL;
	}

	public String getLink() {
		return link;
	}

	public void setLink(String link) {
		this.link = link;
	}

	public DiscoveryServiceEndpoint getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(DiscoveryServiceEndpoint endpoint) {
		this.endpoint = endpoint;
	}

	public Integer getParallelismCount() {
		return parallelismCount;
	}

	public void setParallelismCount(Integer parallelismCount) {
		this.parallelismCount = parallelismCount;
	}

}
