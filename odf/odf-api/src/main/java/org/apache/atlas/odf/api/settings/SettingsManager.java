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
import java.util.Properties;

import org.apache.atlas.odf.api.settings.validation.ValidationException;

/**
*
* External Java API for reading and updating ODF settings
*
*/
public interface SettingsManager {

	/**
	 * Retrieve Kafka consumer properties
	 * @return Current Kafka consumer properties
	 */
	public Properties getKafkaConsumerProperties();

	/**
	 * Retrieve Kafka producer properties
	 * @return Current Kafka producer properties
	 */
	public Properties getKafkaProducerProperties();

	/**
	 * Retrieve overall ODF settings including plain passwords
	 * @return Current ODF settings
	 */
	public ODFSettings getODFSettings();

	/**
	 * Retrieve overall ODF settings with hidden passwords
	 * @return Current ODF settings
	 */
	public ODFSettings getODFSettingsHidePasswords();

	/**
	 * Update ODF settings
	 * 
	 * Passwords provided as plain text will be encrypted. If HIDDEN_PASSWORD_IDENTIFIER
	 * is provided instead of a password, the stored password will remain unchanged.
	 * 
	 * @param Updated ODF settings
	 */
	public void updateODFSettings(ODFSettings update) throws ValidationException;

	/**
	 * Reset ODF settings to the defaults
	 */
	public void resetODFSettings();

	/**
	 * Retrieve user defined ODF properties
	 * @return Map of user defined ODF properties
	 */
	public Map<String, Object> getUserDefinedConfig();

	/**
	 * Update user defined ODF properties
	 * @param Map of user defined ODF properties
	 * @throws ValidationException
	 */
	public void updateUserDefined(Map<String, Object> update) throws ValidationException;
}
