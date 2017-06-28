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
package org.apache.atlas.odf.api.analysis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

// JSON
@ApiModel(description="Request for starting a discovery service.")
public class AnalysisRequest {

	// only used when returned by the ODF
	@ApiModelProperty(value="Unique request id (generated)", readOnly=true, required=false)
	private String id;

	@ApiModelProperty(value="Data set to be analyzed (currently limited to  a single data set)", required=true)
	private List<MetaDataObjectReference> dataSets = new ArrayList<>();

	@ApiModelProperty(value="Sequence of ids (or single id) of the discovery services to be issued", required=false)
	private List<String> discoveryServiceSequence = new ArrayList<String>();

	@ApiModelProperty(value="List annotation types to be created on the dataset(s)", required=false)
	private List<String> annotationTypes = new ArrayList<String>();

	@ApiModelProperty(value="Optional additional properties map to be passed to the discovery service(s)", required=false)
	private Map<String, Object> additionalProperties = new HashMap<String, Object>();

	@ApiModelProperty(value="Indicates that multiple data sets should be processed sequentially rather than in parallel", required=false)
	private boolean processDataSetsSequentially = true;

	// if false the request will fail if some discovery service that cannot process a data set 
	@ApiModelProperty(value="Indicates that access to the data set should not be checked before starting the discovery service", required=false)
	private boolean ignoreDataSetCheck = false;

	public List<MetaDataObjectReference> getDataSets() {
		return dataSets;
	}

	public void setDataSets(List<MetaDataObjectReference> dataSets) {
		this.dataSets = dataSets;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<String> getDiscoveryServiceSequence() {
		return discoveryServiceSequence;
	}

	public void setDiscoveryServiceSequence(List<String> discoveryServiceSequence) {
		this.discoveryServiceSequence = discoveryServiceSequence;
	}

	public List<String> getAnnotationTypes() {
		return annotationTypes;
	}

	public void setAnnotationTypes(List<String> annotationTypes) {
		this.annotationTypes = annotationTypes;
	}

	public Map<String, Object> getAdditionalProperties() {
		return additionalProperties;
	}

	public void setAdditionalProperties(Map<String, Object> additionalProperties) {
		this.additionalProperties = additionalProperties;
	}

	public boolean isProcessDataSetsSequentially() {
		return processDataSetsSequentially;
	}

	public void setProcessDataSetsSequentially(boolean processDataSetsSequentially) {
		this.processDataSetsSequentially = processDataSetsSequentially;
	}

	public boolean isIgnoreDataSetCheck() {
		return ignoreDataSetCheck;
	}

	public void setIgnoreDataSetCheck(boolean ignoreDataSetCheck) {
		this.ignoreDataSetCheck = ignoreDataSetCheck;
	}
}
