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

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.discoveryservice.ServiceNotFoundException;
import org.apache.atlas.odf.api.settings.KafkaMessagingConfiguration;
import org.apache.atlas.odf.api.settings.ODFSettings;
import org.apache.atlas.odf.api.settings.SettingsManager;
import org.apache.atlas.odf.api.settings.validation.ValidationException;
import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.core.Utils;
import org.apache.atlas.odf.core.configuration.ConfigContainer;
import org.apache.atlas.odf.core.configuration.ConfigManager;
import org.apache.atlas.odf.core.test.ODFTestLogger;
import org.apache.atlas.odf.core.test.ODFTestcase;
import org.apache.atlas.odf.json.JSONUtils;
import org.apache.wink.json4j.JSONException;
import org.apache.wink.json4j.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * this test uses a mocked storage therefore no zookeeper is required
 */
public class ODFConfigurationTest extends ODFTestcase {

	Logger logger = ODFTestLogger.get();

	@Before
	public void setupDefaultConfig() throws JsonParseException, JsonMappingException, IOException, ValidationException, JSONException {
		logger.info("reset config to default");
		InputStream is = ODFConfigurationTest.class.getClassLoader().getResourceAsStream("org/apache/atlas/odf/core/test/internal/odf-initial-configuration.json");
		ConfigContainer defaultConfig = new ObjectMapper().readValue(is, ConfigContainer.class);
		ConfigManager configManager = new ODFInternalFactory().create(ConfigManager.class);
		configManager.updateConfigContainer(defaultConfig);
	}

	@Test
	public void testUserDefinedMerge() throws JsonParseException, JsonMappingException, IOException {
		InputStream is = ODFConfigurationTest.class.getClassLoader().getResourceAsStream("org/apache/atlas/odf/core/test/internal/odf-initial-configuration.json");
		ConfigContainer defaultConfig;
		defaultConfig = new ObjectMapper().readValue(is, ConfigContainer.class);
		//set testProps to defaultValues to be overwritten
		defaultConfig.getOdf().getUserDefined().put("testProp", "defaultValue");
		defaultConfig.getOdf().getUserDefined().put("testProp2", "defaultValue");
		logger.info("Read config: " + defaultConfig);

		//config example with userdefined property testProp to 123
		String value = "{\r\n\t\"odf\" : {\r\n\t\"userDefined\" : {\r\n\t\t\"testProp\" : 123\r\n\t}\r\n}\r\n}\r\n";
		ConfigContainer props = new ObjectMapper().readValue(value, ConfigContainer.class);
		Utils.mergeODFPOJOs(defaultConfig, props);
		logger.info("Mergded config: " + defaultConfig);

		Assert.assertEquals(123, defaultConfig.getOdf().getUserDefined().get("testProp"));
		Assert.assertEquals("defaultValue", defaultConfig.getOdf().getUserDefined().get("testProp2"));
	}

	@Test
	public void testValidation() throws JsonParseException, JsonMappingException, IOException {
		boolean exceptionOccured = false;
		String value = "{\r\n\t\"odf\" : {\r\n\t\t\"discoveryServiceWatcherWaitMs\" : -5\r\n\t}\r\n}\r\n";
		try {
			ConfigContainer props = new ObjectMapper().readValue(value, ConfigContainer.class);
			props.validate();
		} catch (ValidationException e) {
			exceptionOccured = true;
		}

		Assert.assertTrue(exceptionOccured);
	}

	@Test
	public void testMerge() throws JsonParseException, JsonMappingException, IOException {
		InputStream is = ODFConfigurationTest.class.getClassLoader().getResourceAsStream("org/apache/atlas/odf/core/test/internal/odf-initial-configuration.json");
		ConfigContainer defaultConfig;
		defaultConfig = new ObjectMapper().readValue(is, ConfigContainer.class);
		//config example with ODF - queueConsumerWaitMs property value 777
		String value = "{\r\n\t\"odf\" : {\r\n\t\t\"discoveryServiceWatcherWaitMs\" : 777\r\n\t}\r\n}\r\n";
		ConfigContainer props = new ObjectMapper().readValue(value, ConfigContainer.class);
		Utils.mergeODFPOJOs(defaultConfig, props);

		// TODOCONFIG, move next line to kafka tests
		// Assert.assertEquals(777, defaultConfig.getOdf().getQueueConsumerWaitMs().intValue());
	}

	@Test
	public void testDeepMerge() throws JsonParseException, JsonMappingException, IOException {
		InputStream is = ODFConfigurationTest.class.getClassLoader().getResourceAsStream("org/apache/atlas/odf/core/test/internal/odf-initial-configuration.json");
		ConfigContainer defaultConfig;
		defaultConfig = new ObjectMapper().readValue(is, ConfigContainer.class);
		//config example with ODF - kafkaConsumer - offsetsStorage property value TEST. All other values for the kafkaConsumer should stay the same!
		String value = "{\r\n\t\"odf\" : {\r\n\"messagingConfiguration\": { \"type\": \"" + KafkaMessagingConfiguration.class.getName()
				+ "\", \t\t\"kafkaConsumerConfig\" : { \r\n\t\t\t\"offsetsStorage\" : \"TEST\"\r\n\t\t}\r\n\t}\r\n}}\r\n";
		ConfigContainer props = new ObjectMapper().readValue(value, ConfigContainer.class);
		Utils.mergeODFPOJOs(defaultConfig, props);

		// TODOCONFIG
		//		Assert.assertEquals("TEST", defaultConfig.getOdf().getKafkaConsumerConfig().getOffsetsStorage());
		//make sure the rest is still default
		//		Assert.assertEquals(400, defaultConfig.getOdf().getKafkaConsumerConfig().getZookeeperSessionTimeoutMs().intValue());
	}

	@Test
	public void testGet() {
		Assert.assertTrue(new ODFFactory().create().getSettingsManager().getODFSettings().isReuseRequests());
	}

	@Test
	public void testPut() throws InterruptedException, IOException, ValidationException, JSONException, ServiceNotFoundException {
		SettingsManager config = new ODFFactory().create().getSettingsManager();
		String propertyId = "my_dummy_test_property";
		int testNumber = 123;
		Map<String, Object> cont = config.getUserDefinedConfig();
		cont.put(propertyId, testNumber);
		config.updateUserDefined(cont);
		Assert.assertEquals(testNumber, config.getUserDefinedConfig().get(propertyId));

		String testString = "test";
		cont.put(propertyId, testString);
		config.updateUserDefined(cont);

		Assert.assertEquals(testString, config.getUserDefinedConfig().get(propertyId));

		JSONObject testJson = new JSONObject();
		testJson.put("testProp", "test");
		cont.put(propertyId, testJson);
		config.updateUserDefined(cont);

		Assert.assertEquals(testJson, config.getUserDefinedConfig().get(propertyId));

		ODFSettings settings = config.getODFSettings();
		logger.info("Last update object: " + JSONUtils.toJSON(settings));
		Assert.assertNotNull(settings);
		Assert.assertNotNull(settings.getUserDefined());
		Assert.assertNotNull(settings.getUserDefined().get(propertyId));
		logger.info("User defined object: " + settings.getUserDefined().get(propertyId).getClass());
		@SuppressWarnings("unchecked")
		Map<String, Object> notifiedNestedJSON = (Map<String, Object>) settings.getUserDefined().get(propertyId);
		Assert.assertNotNull(notifiedNestedJSON.get("testProp"));
		Assert.assertTrue(notifiedNestedJSON.get("testProp") instanceof String);
		Assert.assertEquals("test", notifiedNestedJSON.get("testProp"));
	}
}
