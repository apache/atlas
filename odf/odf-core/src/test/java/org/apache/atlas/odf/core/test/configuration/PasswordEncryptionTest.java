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
package org.apache.atlas.odf.core.test.configuration;

import java.util.logging.Logger;

import org.junit.Assert;
import org.junit.Test;

import org.apache.atlas.odf.core.Encryption;
import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.settings.SettingsManager;
import org.apache.atlas.odf.api.settings.SparkConfig;
import org.apache.atlas.odf.api.settings.ODFSettings;
import org.apache.atlas.odf.core.test.ODFTestLogger;
import org.apache.atlas.odf.core.test.TimerTestBase;
import org.apache.atlas.odf.json.JSONUtils;

public class PasswordEncryptionTest extends TimerTestBase {
	Logger logger = ODFTestLogger.get();
	private static final String SPARK_PASSWORD_CONFIG = "spark.authenticate.secret";

	@Test
	public void testGeneralPasswordEncryption() throws Exception {
		SettingsManager settings = new ODFFactory().create().getSettingsManager();
		ODFSettings settingsWithPlainPasswords = settings.getODFSettingsHidePasswords();
		settingsWithPlainPasswords.setOdfPassword("newOdfPassword");
		logger.info("Settings with plain password: " + JSONUtils.toJSON(settingsWithPlainPasswords));
		settings.updateODFSettings(settingsWithPlainPasswords);

		ODFSettings settingsWithHiddenPasswords = settings.getODFSettingsHidePasswords();
		String hiddenPasswordIdentifyier = "***hidden***";
		Assert.assertEquals(hiddenPasswordIdentifyier, settingsWithHiddenPasswords.getOdfPassword());
		logger.info("Settings with hidden password: " + JSONUtils.toJSON(settingsWithHiddenPasswords));

		ODFSettings settingsWithEncryptedPassword = settings.getODFSettings();
		Assert.assertEquals("newOdfPassword", Encryption.decryptText(settingsWithEncryptedPassword.getOdfPassword()));
		logger.info("Settings with encrypted password: " + JSONUtils.toJSON(settingsWithEncryptedPassword));

		// When overwriting settings with hidden passwords, encrypted passwords must be kept internally
		settings.updateODFSettings(settingsWithHiddenPasswords);
		settingsWithEncryptedPassword = settings.getODFSettings();
		Assert.assertEquals("newOdfPassword", Encryption.decryptText(settingsWithEncryptedPassword.getOdfPassword()));
	}

	@Test
	public void testSparkConfigEncryption() throws Exception {
		SettingsManager settings = new ODFFactory().create().getSettingsManager();
		SparkConfig plainSparkConfig = new SparkConfig();
		plainSparkConfig.setConfig(SPARK_PASSWORD_CONFIG, "plainConfigValue");
		ODFSettings settingsWithPlainPasswords = settings.getODFSettings();
		settingsWithPlainPasswords.setSparkConfig(plainSparkConfig);;
		logger.info("Settings with plain password: " + JSONUtils.toJSON(settingsWithPlainPasswords));
		settings.updateODFSettings(settingsWithPlainPasswords);

		ODFSettings settingsWithHiddenPasswords = settings.getODFSettingsHidePasswords();
		String hiddenPasswordIdentifyier = "***hidden***";
		String hiddenConfigValue = (String) settingsWithHiddenPasswords.getSparkConfig().getConfigs().get(SPARK_PASSWORD_CONFIG);
		Assert.assertEquals(hiddenPasswordIdentifyier, hiddenConfigValue);
		logger.info("Config with hidden password: " + JSONUtils.toJSON(settingsWithHiddenPasswords));

		ODFSettings settingsWithEncryptedPassword = settings.getODFSettings();
		String encryptedConfigValue = (String) settingsWithEncryptedPassword.getSparkConfig().getConfigs().get(SPARK_PASSWORD_CONFIG);
		Assert.assertEquals("plainConfigValue", Encryption.decryptText(encryptedConfigValue));
		logger.info("Config with encrypted password: " + JSONUtils.toJSON(settingsWithEncryptedPassword));

		// When overwriting settings with hidden passwords, encrypted passwords must be kept internally
		settings.updateODFSettings(settingsWithHiddenPasswords);
		encryptedConfigValue = (String) settingsWithEncryptedPassword.getSparkConfig().getConfigs().get(SPARK_PASSWORD_CONFIG);
		Assert.assertEquals("plainConfigValue", Encryption.decryptText(encryptedConfigValue));
	}
}
