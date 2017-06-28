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
package org.apache.atlas.odf.core.store.zookeeper34.test;

import org.apache.atlas.odf.api.settings.ODFSettings;
import org.apache.atlas.odf.core.configuration.ConfigContainer;
import org.apache.atlas.odf.core.store.zookeeper34.ZookeeperConfigurationStorage;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * this test uses the real storage implementation therefore a zookeeper is required
 */
public class ZookeeperConfigurationStorageTest {
	@BeforeClass
	public static void setup() {
		new TestZookeeper().start();
	}

	@Test
	public void testStoreInZookeeper() {
		ZookeeperConfigurationStorage store = new ZookeeperConfigurationStorage() {

			@Override
			public String getZookeeperConfigPath() {
				return "/odf/testconfig";
			}
			
		};
		ConfigContainer container = new ConfigContainer();
		ODFSettings odfConfig = new ODFSettings();
		String instanceId = "my_test_id";
		odfConfig.setInstanceId(instanceId);
		container.setOdf(odfConfig);
		store.storeConfig(container);

		ConfigContainer updatedContainer = store.getConfig(null);
		Assert.assertEquals(instanceId, updatedContainer.getOdf().getInstanceId());
		store.clearCache();
		
	}
}
