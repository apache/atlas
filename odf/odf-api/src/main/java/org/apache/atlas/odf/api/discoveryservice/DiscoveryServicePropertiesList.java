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
 * JSON object representing properties of registered discovery services.
 *  
 */

@ApiModel(description="List of properties of registered discovery services")
public class DiscoveryServicePropertiesList {
	
	@ApiModelProperty(value="List of properties of registered discovery services", readOnly=true)
	DiscoveryServiceProperties[] items;

	@ApiModelProperty(value="Number of items in the list", readOnly=true)
	int count;

}
