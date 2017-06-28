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
package org.apache.atlas.odf.core.configuration;


import java.util.List;

import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceProperties;
import org.apache.atlas.odf.api.settings.ODFSettings;
import org.apache.atlas.odf.api.settings.validation.ValidationException;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

/**
 * { 
 *  	"odf" : {...},
 *  	"userDefined" : {...}
 * }
 *
 *
 * This class is final, because reflection is used to access getters / setters in order to merge. This doesn't work with inherited methods
 */
@ApiModel(description="All ODF configuration options.")
public final class ConfigContainer {

	@ApiModelProperty(value="General ODF configuration options along with details about available discovery services", required=true)
	private ODFSettings odf;

	@ApiModelProperty(value="Details about available discovery services")
	private List<DiscoveryServiceProperties> registeredServices = null;

	public List<DiscoveryServiceProperties> getRegisteredServices() {
		return registeredServices;
	}

	public void setRegisteredServices(List<DiscoveryServiceProperties> registeredServices) {
		this.registeredServices = registeredServices;
	}

	public ODFSettings getOdf() {
		return odf;
	}

	public void setOdf(ODFSettings odfSettings) {
		this.odf = odfSettings;
	}

	public void validate() throws ValidationException {
		if (this.odf != null) {
			odf.validate();
		}
		if (this.registeredServices != null) {
			new ServiceValidator().validate("ODFConfig.registeredServices", this.registeredServices);
		}
	}
}
