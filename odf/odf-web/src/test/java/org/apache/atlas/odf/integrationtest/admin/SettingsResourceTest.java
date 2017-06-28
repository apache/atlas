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
package org.apache.atlas.odf.integrationtest.admin;

import org.apache.atlas.odf.api.settings.MessagingConfiguration;
import org.apache.atlas.odf.api.settings.ODFSettings;
import org.apache.atlas.odf.core.Encryption;
import org.apache.atlas.odf.rest.test.RestTestBase;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.Test;

import org.apache.atlas.odf.api.settings.KafkaMessagingConfiguration;
import org.apache.atlas.odf.json.JSONUtils;

public class SettingsResourceTest extends RestTestBase {

	@Test
	public void testSettingsRead() throws Exception {
		ODFSettings settings = settingsRead();
		Assert.assertNotNull(settings);
		MessagingConfiguration msgConfig = settings.getMessagingConfiguration();
		Assert.assertNotNull(msgConfig);
		Assert.assertTrue(msgConfig instanceof KafkaMessagingConfiguration);
		KafkaMessagingConfiguration kafkaMsgConfig = (KafkaMessagingConfiguration) msgConfig;
		Assert.assertNotNull(kafkaMsgConfig.getKafkaConsumerConfig());
		Assert.assertNotNull(kafkaMsgConfig.getKafkaConsumerConfig().getZookeeperConnectionTimeoutMs());

		Assert.assertNotNull(settings.getUserDefined());
	}

	@Test
	public void testPasswordEncryption() throws Exception {
		ODFSettings settings = settingsRead();
		settings.setOdfPassword("newOdfPassword");
		ODFSettings configWithPlainPasswords = settings;
		settingsWrite(JSONUtils.toJSON(configWithPlainPasswords), HttpStatus.SC_OK);
		logger.info("Settings with plain password: " + JSONUtils.toJSON(configWithPlainPasswords));

		// REST API must return hidden password
		ODFSettings configWithHiddenPasswords = settingsRead();
		String hiddenPasswordIdentifyier = "***hidden***";
		Assert.assertEquals(configWithHiddenPasswords.getOdfPassword(), hiddenPasswordIdentifyier);

		// Reset passwords
		Assert.assertNotNull(System.getProperty("odf.test.password"));
		settings = settingsRead();
		settings.setOdfPassword(Encryption.decryptText(System.getProperty("odf.test.password")));
		settingsWrite(JSONUtils.toJSON(settings), HttpStatus.SC_OK);
	}

	@Test
	public void testSettingsWriteSuccess() throws Exception {
		String configSnippet = "{ \"runAnalysisOnImport\": false }";
		logger.info("Testing write settings success with JSON: " + configSnippet);
		settingsWrite(configSnippet, HttpStatus.SC_OK);
	}
	
	@Test
	public void testSettingsWriteFailure() throws Exception {
		String configSnippet = "{ \"runAnalysisOnImport\": \"someInvalidValue\" }";
		logger.info("Testing write settings failure with JSON: " + configSnippet);
		settingsWrite(configSnippet, HttpStatus.SC_INTERNAL_SERVER_ERROR);
	}

	@Test
	public void testSettingsReset() throws Exception {
		logger.info("Testing reset settings operation.");
		String updatedId = "updatedInstanceId";
		ODFSettings originalConfig = settingsRead();
		String originalInstanceId = originalConfig.getInstanceId();
		originalConfig.setInstanceId(updatedId);

		settingsWrite(JSONUtils.toJSON(originalConfig), HttpStatus.SC_OK);
		
		ODFSettings newConfig = settingsRead();
		Assert.assertEquals(updatedId, newConfig.getInstanceId());

		settingsReset();

		ODFSettings resetConfig = settingsRead();
		String resetInstanceId = resetConfig.getInstanceId();

		Assert.assertEquals(originalInstanceId, resetInstanceId);
	}
}
