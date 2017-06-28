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

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.core.controlcenter.AdminMessage;
import org.apache.atlas.odf.core.controlcenter.AdminMessage.Type;
import org.apache.atlas.odf.core.messaging.DiscoveryServiceQueueManager;
import org.apache.atlas.odf.json.JSONUtils;

public class ODFConfigNotificationPublisher {

	Logger logger = Logger.getLogger(ODFConfigNotificationPublisher.class.getName());

	public void publishConfigChange(ConfigContainer update, String changeId) {
		try {
			logger.log(Level.FINE, "publishing config change: {0}", JSONUtils.toJSON(update));
			ConfigContainer clone = JSONUtils.fromJSON(JSONUtils.toJSON(update), ConfigContainer.class);
			AdminMessage amsg = new AdminMessage();
			amsg.setId(changeId);
			amsg.setAdminMessageType(Type.CONFIGCHANGE);
			amsg.setConfigUpdateDetails(clone);
			amsg.setDetails("Configuration update");
			DiscoveryServiceQueueManager qm = new ODFInternalFactory().create(DiscoveryServiceQueueManager.class);
			qm.enqueueInAdminQueue(amsg);
		} catch (Exception exc) {
			logger.log(Level.WARNING, "An unexpected exception occurres when writing to admin queue. Ignoring it", exc);
		}
	}

}
