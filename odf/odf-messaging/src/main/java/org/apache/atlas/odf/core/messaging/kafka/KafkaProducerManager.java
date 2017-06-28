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
package org.apache.atlas.odf.core.messaging.kafka;

import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TimeoutException;

import org.apache.atlas.odf.core.Environment;
import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.core.messaging.MessageEncryption;
import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.settings.SettingsManager;

public class KafkaProducerManager {

	private final static Logger logger = Logger.getLogger(KafkaProducerManager.class.getName());
	private static KafkaProducer<String, String> producer;

	protected Properties getKafkaProducerConfig() {
		SettingsManager odfConfig = new ODFFactory().create().getSettingsManager();
		ODFInternalFactory f = new ODFInternalFactory();
		Properties props = odfConfig.getKafkaProducerProperties();
		String zookeeperConnect = f.create(Environment.class).getZookeeperConnectString();
		final Iterator<String> brokers = f.create(KafkaMonitor.class).getBrokers(zookeeperConnect).iterator();
		StringBuilder brokersString = new StringBuilder();
		while (brokers.hasNext()) {
			brokersString.append(brokers.next());
			if (brokers.hasNext()) {
				brokersString.append(",");
			}
		}
		logger.info("Sending messages to brokers: " + brokersString.toString());
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokersString.toString());
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "ODF_MESSAGE_PRODUCER");
		return props;
	}

	private KafkaProducer<String, String> getProducer() {
		if (producer == null) {
			producer = new KafkaProducer<String, String>(getKafkaProducerConfig());
		}
		return producer;
	}

	public void sendMsg(String topicName, String key, String value) {
		MessageEncryption msgEncryption = new ODFInternalFactory().create(MessageEncryption.class);
		value = msgEncryption.encrypt(value);
		sendMsg(topicName, key, value, null);
	}

	public void sendMsg(final String topicName, final String key, final String value, final Callback callback) {
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topicName, key, value);
		try {
			int retryCount = 0;
			boolean msgSend = false;
			while (retryCount < 5 && !msgSend) {
				try {
					getProducer().send(producerRecord, callback).get(4000, TimeUnit.MILLISECONDS);
					msgSend = true;
				} catch (ExecutionException ex) {
					if (ex.getCause() instanceof TimeoutException) {
						logger.warning("Message could not be send within 4000 ms");
						retryCount++;
					} else {
						throw ex;
					}

				}
			}
			if (retryCount == 5) {
				logger.warning("Message could not be send within 5 retries!");
				logger.fine("topic: " + topicName + " key " + key + " msg " + value);
			}
		} catch (Exception exc) {
			logger.log(Level.WARNING, "Exception while sending message", exc);
			if (producer != null) {
				producer.close();
			}
			producer = null;
			throw new RuntimeException(exc);
		}
	}

}
