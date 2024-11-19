/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package org.apache.atlas.flink.hook;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.flink.model.FlinkDataTypes;
import org.apache.atlas.model.instance.AtlasEntity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class KafkaEntities {

	public static void addKafkaSourceEntity(FlinkKafkaConsumer kafkaSource, List<AtlasEntity> ret, String metadataNamespace) {
		KafkaTopicsDescriptor topicsDescriptor = kafkaSource.getTopicsDescriptor();
		Properties kafkaProps = kafkaSource.getProperties();

		List<String> topics = topicsDescriptor.isFixedTopics() ? topicsDescriptor.getFixedTopics() : Collections.singletonList(topicsDescriptor.getTopicPattern().toString());
		String uri = kafkaProps.getProperty("bootstrap.servers");

		for (String topic : topics) {
			AtlasEntity e = new AtlasEntity(FlinkDataTypes.KAFKA_TOPIC.getName());

			e.setAttribute("topic", topic);
			e.setAttribute("uri", uri);
			e.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, getKafkaTopicQualifiedName(metadataNamespace, topic));
			e.setAttribute(AtlasClient.NAME, topic);
			ret.add(e);
		}
	}

	public static void addKafkaSinkEntity(FlinkKafkaProducer kafkaSink, List<AtlasEntity> ret, String metadataNamespace) {
		KafkaTopicsDescriptor topicsDescriptor = kafkaSink.getTopicsDescriptor();
		Properties kafkaProps = kafkaSink.getProperties();

		List<String> topics = new ArrayList<>();
		if (topicsDescriptor.isFixedTopics()) {
			topics.addAll(topicsDescriptor.getFixedTopics());
		} else {
			topics.add(topicsDescriptor.getTopicPattern().toString());
		}

		String uri = kafkaProps.getProperty("bootstrap.servers");

		for (String topic : topics) {
			AtlasEntity e = new AtlasEntity(FlinkDataTypes.KAFKA_TOPIC.getName());

			e.setAttribute("topic", topic);
			e.setAttribute("uri", uri);
			e.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, getKafkaTopicQualifiedName(metadataNamespace, topic));
			e.setAttribute(AtlasClient.NAME, topic);
			ret.add(e);
		}
	}

	private static String getKafkaTopicQualifiedName(String metadataNamespace, String topicName) {
		return String.format("%s@%s", topicName.toLowerCase(), metadataNamespace);
	}
}
