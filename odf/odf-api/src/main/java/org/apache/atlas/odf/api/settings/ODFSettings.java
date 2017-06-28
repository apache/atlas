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
package org.apache.atlas.odf.api.settings;

import java.util.Map;
import org.apache.atlas.odf.api.settings.validation.NumberPositiveValidator;
import org.apache.atlas.odf.api.settings.validation.StringNotEmptyValidator;
import org.apache.atlas.odf.api.settings.validation.ValidationException;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

/*
 * 
 * This class is final, because reflection is used to access getters / setters in order to merge. This doesn't work with inherited methods
 * Boolean properties must be of type Boolean instead of boolean in order to support null values which are required for merging later!
 *
 */
@ApiModel(description="General ODF settings.")
public final class ODFSettings {

	/*
	 * ############ !!!!!!!!!!!!!!!!!!! ####################
	 *
	 * Because of a jackson defect, JsonProperty annotations must be on all properties AND their getters and setters!
	 *
	 * https://github.com/FasterXML/jackson-module-scala/issues/197
	 */

	@ApiModelProperty(value="Polling interval for checking whether a discovery service is still running")
	private Integer discoveryServiceWatcherWaitMs;

	@ApiModelProperty(value="Unique id of the ODF instance")
	private String instanceId;

	@ApiModelProperty(value="ODF URL passed to discovery services for remote access to the metadata API")
	private String odfUrl;

	@ApiModelProperty(value="ODF user id passed to discovery services for remote access to the metadata API")
	private String odfUser;

	@ApiModelProperty(value="ODF password passed to discovery services for remote access to the metadata API")
	private String odfPassword;

	@ApiModelProperty(value = "ATLAS setting indicating if events regarding newly imported data sets should be consumed from me")
	private Boolean consumeMessageHubEvents;

	@ApiModelProperty(value = "ATLAS Messagehub VCAP_SERVICES value from Bluemix,  e.g { \"messagehub\": [{\"name\" : \"...\",\n\"credentials\": {...}]")
	private String atlasMessagehubVcap;

	@ApiModelProperty(value="Indicates whether to reuse equivalent analysis requests that may be already queued rather that running the same analysis again")
	private Boolean reuseRequests;

	@ApiModelProperty(value="Messaging configuration to be used for queuing requests")
	private MessagingConfiguration messagingConfiguration;

	@ApiModelProperty(value="If set to true, ALL registered discovery services will be automatically issued when a new data set is imported")
	private Boolean runAnalysisOnImport;

	@ApiModelProperty(value="If set to true, ALL data sets will be automatically analyzed whenever a new discovery service is registered")
	private Boolean runNewServicesOnRegistration;

	@ApiModelProperty(value="User-defined configuration options for discovery services", required=true)
	private Map<String, Object> userDefined;

	@ApiModelProperty(value="Spark clusters to be used for running discovery services", required=true)
	private SparkConfig sparkConfig;

	@ApiModelProperty(value = "Set to true to propagate the created annotations for each analysis request to the metadata store")
	private Boolean enableAnnotationPropagation;

	public Boolean getEnableAnnotationPropagation() {
		return enableAnnotationPropagation;
	}

	public void setEnableAnnotationPropagation(Boolean enableAnnotationPropagation) {
		this.enableAnnotationPropagation = enableAnnotationPropagation;
	}

	public Boolean isReuseRequests() {
		return reuseRequests;
	}

	public void setReuseRequests(Boolean reuseRequests) {
		this.reuseRequests = reuseRequests;
	}

	public String getInstanceId() {
		return this.instanceId;
	}

	public void setInstanceId(String instanceId) {
		this.instanceId = instanceId;
	}

	public String getOdfUrl() {
		return this.odfUrl;
	}

	public void setOdfUrl(String odfUrl) {
		this.odfUrl = odfUrl;
	}

	public String getOdfUser() {
		return this.odfUser;
	}

	public void setOdfUser(String odfUser) {
		this.odfUser = odfUser;
	}

	public String getOdfPassword() {
		return this.odfPassword;
	}

	public void setOdfPassword(String odfPassword) {
		this.odfPassword = odfPassword;
	}

	public Integer getDiscoveryServiceWatcherWaitMs() {
		return discoveryServiceWatcherWaitMs;
	}

	public void setDiscoveryServiceWatcherWaitMs(Integer discoveryServiceWatcherWaitMs) {
		this.discoveryServiceWatcherWaitMs = discoveryServiceWatcherWaitMs;
	}

	public Boolean getRunAnalysisOnImport() {
		return runAnalysisOnImport;
	}

	public void setRunAnalysisOnImport(Boolean runAnalysisOnImport) {
		this.runAnalysisOnImport = runAnalysisOnImport;
	}

	public Boolean getRunNewServicesOnRegistration() {
		return runNewServicesOnRegistration;
	}

	public void setRunNewServicesOnRegistration(Boolean runNewServicesOnRegistration) {
		this.runNewServicesOnRegistration = runNewServicesOnRegistration;
	}

	public MessagingConfiguration getMessagingConfiguration() {
		return messagingConfiguration;
	}

	public void setMessagingConfiguration(MessagingConfiguration messagingConfiguration) {
		this.messagingConfiguration = messagingConfiguration;
	}

	public String getAtlasMessagehubVcap() {
		return atlasMessagehubVcap;
	}

	public void setAtlasMessagehubVcap(String atlasMessagehubVcap) {
		this.atlasMessagehubVcap = atlasMessagehubVcap;
	}

	public Map<String, Object> getUserDefined() {
		return userDefined;
	}

	public Boolean getConsumeMessageHubEvents() {
		return consumeMessageHubEvents;
	}

	public void setConsumeMessageHubEvents(Boolean consumeMessageHubEvents) {
		this.consumeMessageHubEvents = consumeMessageHubEvents;
	}

	public void setUserDefined(Map<String, Object> userDefined) {
		this.userDefined = userDefined;
	}

	public SparkConfig getSparkConfig() {
		return sparkConfig;
	}

	public void setSparkConfig(SparkConfig sparkConfig) {
		this.sparkConfig = sparkConfig;
	}

	public void validate() throws ValidationException {
		new StringNotEmptyValidator().validate("ODFConfig.instanceId", instanceId);

		if (this.getDiscoveryServiceWatcherWaitMs() != null) {
			new NumberPositiveValidator().validate("ODFConfig.discoveryServiceWatcherWaitMs", this.discoveryServiceWatcherWaitMs);
		}

		if (this.messagingConfiguration != null) {
			this.messagingConfiguration.validate();
		}
	}
}
