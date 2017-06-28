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
package org.apache.atlas.odf.core.controlcenter;

import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.core.controlcenter.AdminMessage.Type;
import org.apache.atlas.odf.core.store.ODFConfigurationStorage;
import org.apache.atlas.odf.json.JSONUtils;

public class ConfigChangeQueueProcessor implements QueueMessageProcessor {

	Logger logger = Logger.getLogger(ConfigChangeQueueProcessor.class.getName());
	
	@Override
	public void process(ExecutorService executorService, String msg, int partition, long offset) {
		try {
			AdminMessage amsg = JSONUtils.fromJSON(msg, AdminMessage.class);
			if (Type.CONFIGCHANGE.equals(amsg.getAdminMessageType())) {
				logger.info("Received config change: " + JSONUtils.toJSON(amsg));
				ODFInternalFactory f = new ODFInternalFactory();
				ODFConfigurationStorage configStorage = f.create(ODFConfigurationStorage.class);
				configStorage.onConfigChange(amsg.getConfigUpdateDetails());
				configStorage.removePendingConfigChange(amsg.getId());
			}
		} catch(Exception exc) {
			logger.log(Level.WARNING, "An exception occurred while processing admin message", exc);
		}
	}

}
