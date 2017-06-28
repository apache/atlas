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
package org.apache.atlas.odf.core.settings;

import java.util.Map;
import java.util.Properties;

import org.apache.wink.json4j.JSONException;
import org.apache.wink.json4j.JSONObject;

import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.core.configuration.ConfigContainer;
import org.apache.atlas.odf.core.configuration.ConfigManager;
import org.apache.atlas.odf.api.settings.SettingsManager;
import org.apache.atlas.odf.api.settings.KafkaConsumerConfig;
import org.apache.atlas.odf.api.settings.KafkaMessagingConfiguration;
import org.apache.atlas.odf.api.settings.MessagingConfiguration;
import org.apache.atlas.odf.api.settings.ODFSettings;
import org.apache.atlas.odf.api.settings.validation.ValidationException;
import org.apache.atlas.odf.json.JSONUtils;

/**
*
* External Java API for reading and updating ODF settings
*
*/
public class SettingsManagerImpl implements SettingsManager {
	public static final String HIDDEN_PASSWORD_IDENTIFIER = "***hidden***";
	private ConfigManager configManager;

	public SettingsManagerImpl() {
		ODFInternalFactory f = new ODFInternalFactory();
		configManager = f.create(ConfigManager.class);
	}

	/**
	 * Retrieve Kafka consumer properties
	 * @return Current Kafka consumer properties
	 */
	public Properties getKafkaConsumerProperties() {
		Properties props = new Properties();
		MessagingConfiguration messagingConfig = getODFSettings().getMessagingConfiguration();
		if (!(messagingConfig instanceof KafkaMessagingConfiguration)) {
			return props;
		}
		KafkaConsumerConfig config = ((KafkaMessagingConfiguration) messagingConfig).getKafkaConsumerConfig();
		try {
			JSONObject configJSON = JSONUtils.toJSONObject(config);
			for (Object key : configJSON.keySet()) {
				props.setProperty((String) key, String.valueOf(configJSON.get(key)));
			}
		} catch (JSONException e) {
			throw new RuntimeException("The kafka consumer config could not be parsed!", e);
		}
		return props;
	}

	/**
	 * Retrieve Kafka producer properties
	 * @return Current Kafka producer properties
	 */
	public Properties getKafkaProducerProperties() {
		// Currently no producer properties are editable and therefore not
		// stored in the config file
		Properties props = new Properties();
		props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return props;
	}

	/**
	 * Retrieve overall ODF settings including plain passwords
	 * @return Current ODF settings
	 */
	public ODFSettings getODFSettings() {
		return configManager.getConfigContainer().getOdf();
	}

	/**
	 * Retrieve overall ODF settings with hidden passwords
	 * @return Current ODF settings
	 */
	public ODFSettings getODFSettingsHidePasswords() {
		return this.configManager.getConfigContainerHidePasswords().getOdf();
	}

	/**
	 * Update ODF settings
	 * 
	 * Passwords provided as plain text will be encrypted. If HIDDEN_PASSWORD_IDENTIFIER
	 * is provided instead of a password, the stored password will remain unchanged.
	 * 
	 * @param Updated ODF settings
	 */
	public void updateODFSettings(ODFSettings update) throws ValidationException {
		ConfigContainer cont = new ConfigContainer();
		cont.setOdf(update);
		this.configManager.updateConfigContainer(cont);
	}

	/**
	 * Reset ODF settings to the defaults
	 */
	public void resetODFSettings() {
		new ODFInternalFactory().create(ConfigManager.class).resetConfigContainer();
	}

	/**
	 * Retrieve user defined ODF properties
	 * @return Map of user defined ODF properties
	 */
	public Map<String, Object> getUserDefinedConfig() {
		return getODFSettings().getUserDefined();
	}

	/**
	 * Update user defined ODF properties
	 * @param Map of user defined ODF properties
	 * @throws ValidationException
	 */
	public void updateUserDefined(Map<String, Object> update) throws ValidationException {
		ODFSettings odfConfig = new ODFSettings();
		odfConfig.setUserDefined(update);
		updateODFSettings(odfConfig);
	}
}
