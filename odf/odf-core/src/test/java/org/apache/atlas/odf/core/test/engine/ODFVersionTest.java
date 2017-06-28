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
package org.apache.atlas.odf.core.test.engine;

import org.junit.Assert;
import org.junit.Test;

import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.engine.ODFVersion;
import org.apache.atlas.odf.core.test.TimerTestBase;

public class ODFVersionTest extends TimerTestBase {
	@Test
	public void testVersion() {
		ODFVersion version = new ODFFactory().create().getEngineManager().getVersion();
		Assert.assertNotNull(version);
		Assert.assertTrue(version.getVersion().startsWith("1.2.0-"));
	}
}
