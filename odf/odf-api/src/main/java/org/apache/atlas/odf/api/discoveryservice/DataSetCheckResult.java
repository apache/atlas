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

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

/**
 * 
 * An object of this class must be returned by a services checkDataSet method.
 *
 */
@ApiModel(description="Result returned by REST-based discovery services that indicates whether a dataset can be processed by the service.")
public class DataSetCheckResult {

	public static enum DataAccess {
		NotPossible,
		Possible
	};

	@ApiModelProperty(value="Indicates whether a dataset can be accessed by a discovery service, i.e. whether access is possible or not", readOnly=true, required=true)
	private DataAccess dataAccess = DataAccess.Possible;

	@ApiModelProperty(value="Message explaining why access to the dataset is not possible", readOnly=true)
	private String details;

	public DataAccess getDataAccess() {
		return dataAccess;
	}

	public void setDataAccess(DataAccess dataAccess) {
		this.dataAccess = dataAccess;
	}

	public String getDetails() {
		return details;
	}

	public void setDetails(String details) {
		this.details = details;
	}

}
