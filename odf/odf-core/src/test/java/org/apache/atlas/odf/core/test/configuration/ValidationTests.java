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

import java.util.Collections;

import org.apache.atlas.odf.api.settings.validation.EnumValidator;
import org.apache.atlas.odf.api.settings.validation.ValidationException;
import org.junit.Assert;
import org.junit.Test;

import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.core.configuration.ConfigContainer;
import org.apache.atlas.odf.core.configuration.ConfigManager;
import org.apache.atlas.odf.core.configuration.ServiceValidator;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceManager;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceProperties;
import org.apache.atlas.odf.api.settings.validation.ImplementationValidator;
import org.apache.atlas.odf.api.settings.validation.NumberPositiveValidator;
import org.apache.atlas.odf.api.settings.validation.PropertyValidator;
import org.apache.atlas.odf.core.test.TimerTestBase;
import org.apache.atlas.odf.core.test.discoveryservice.TestAsyncDiscoveryServiceWritingAnnotations1;
import org.apache.atlas.odf.json.JSONUtils;

public class ValidationTests extends TimerTestBase {

	@Test
	public void testEnum() {
		String[] vals = new String[] { "test", "test2" };
		String correct = "test";
		String incorrect = "fail";

		Assert.assertTrue(validateTest(correct, new EnumValidator(vals)));
		Assert.assertFalse(validateTest(incorrect, new EnumValidator(vals)));
	}

	@Test
	public void testImplementation() {
		String correct = TestAsyncDiscoveryServiceWritingAnnotations1.class.getName();
		String incorrect = "dummyClass";
		Assert.assertTrue(validateTest(correct, new ImplementationValidator()));
		Assert.assertFalse(validateTest(incorrect, new ImplementationValidator()));
	}

	@Test
	public void testService() throws Exception {
		String s = "{\r\n" + 
				"			\"id\": \"asynctestservice\",\r\n" + 
				"			\"name\": \"Async test\",\r\n" + 
				"			\"description\": \"The async test service\",\r\n" + 
				"			\"endpoint\": {\r\n" + 
				"				\"runtimeName\": \"Java\",\r\n" + 
				"				\"className\": \"org.apache.atlas.odf.core.test.discoveryservice.TestAsyncDiscoveryService1\"\r\n" +
				"			}\r\n" + 
				"		}";
		
		DiscoveryServiceProperties newService = JSONUtils.fromJSON(s, DiscoveryServiceProperties.class);
		DiscoveryServiceManager discoveryServicesManager = new ODFFactory().create().getDiscoveryServiceManager();
		//ODFConfig odfConfig = new ODFFactory().create(ODFConfiguration.class).getODFConfig();

		ConfigContainer new1 = new ConfigContainer();
		new1.setRegisteredServices(Collections.singletonList(newService));
		ConfigManager configManager = new ODFInternalFactory().create(ConfigManager.class);
		configManager.updateConfigContainer(new1);
		
		DiscoveryServiceProperties correct = discoveryServicesManager.getDiscoveryServicesProperties().get(0);
		Assert.assertEquals("asynctestservice", correct.getId());
		correct.setId("newId");
		DiscoveryServiceProperties incorrect = new DiscoveryServiceProperties();
		Assert.assertTrue(validateTest(correct, new ServiceValidator()));
		Assert.assertFalse(validateTest(incorrect, new ServiceValidator()));
	}

	@Test
	public void testNumber() {
		int correct = 5;
		int incorrect = -5;
		Assert.assertTrue(validateTest(correct, new NumberPositiveValidator()));
		Assert.assertFalse(validateTest(incorrect, new NumberPositiveValidator()));
	}

	private boolean validateTest(Object value, PropertyValidator validator) {
		try {
			validator.validate(null, value);
			return true;
		} catch (ValidationException ex) {
			return false;
		}
	}

}
