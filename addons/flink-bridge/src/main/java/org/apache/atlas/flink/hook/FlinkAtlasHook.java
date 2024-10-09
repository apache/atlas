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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.DetachedJobExecutionResult;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.source.ContinuousFileMonitoringFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasConstants;
import org.apache.atlas.flink.model.FlinkDataTypes;
import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.type.AtlasTypeUtil;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class FlinkAtlasHook extends AtlasHook implements JobListener {

	public static final Logger LOG = org.slf4j.LoggerFactory.getLogger(FlinkAtlasHook.class);

	public static final String RELATIONSHIP_DATASET_PROCESS_INPUTS = "dataset_process_inputs";
	public static final String RELATIONSHIP_PROCESS_DATASET_OUTPUTS = "process_dataset_outputs";

	private AtlasEntity flinkApp;
	private AtlasEntity.AtlasEntitiesWithExtInfo entity;

	@Override
	public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
		if (throwable != null || !(jobClient.getPipeline() instanceof StreamGraph)) {
			return;
		}
		StreamGraph streamGraph = (StreamGraph) jobClient.getPipeline();

		LOG.info("Collecting metadata for a new Flink Application: {}", streamGraph.getJobName());

		try {
			flinkApp = createAppEntity(streamGraph, jobClient.getJobID());
			entity = new AtlasEntity.AtlasEntitiesWithExtInfo(flinkApp);
			addInputsOutputs(streamGraph, flinkApp, entity);

			notifyEntities(Collections.singletonList(new HookNotification.EntityCreateRequestV2(getUser(), entity)), null);
		} catch (Exception e) {
			throw new RuntimeException("Atlas hook is unable to process the application.", e);
		}
	}

	@Override
	public void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {
		if (throwable != null || jobExecutionResult instanceof DetachedJobExecutionResult) {
			return;
		}
		flinkApp.setAttribute("endTime", new Date(System.currentTimeMillis()));
		notifyEntities(Collections.singletonList(new HookNotification.EntityCreateRequestV2(getUser(), entity)), null);
	}

	private AtlasEntity createAppEntity(StreamGraph graph, JobID jobId) {
		AtlasEntity flinkApp = new AtlasEntity(FlinkDataTypes.FLINK_APPLICATION.getName());
		flinkApp.setAttribute("id", jobId.toString());

		flinkApp.setAttribute(AtlasClient.NAME, graph.getJobName());
		flinkApp.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, graph.getJobName());
		flinkApp.setAttribute("startTime", new Date(System.currentTimeMillis()));
		flinkApp.setAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, getMetadataNamespace());
		flinkApp.setAttribute(AtlasClient.OWNER, getUser());

		return flinkApp;
	}

	private void addInputsOutputs(StreamGraph streamGraph, AtlasEntity flinkApp, AtlasEntity.AtlasEntitiesWithExtInfo entity) {
		List<StreamNode> sources = streamGraph
				.getSourceIDs()
				.stream()
				.map(streamGraph::getStreamNode)
				.collect(Collectors.toList());

		addSourceEntities(sources, flinkApp, entity);

		List<StreamNode> sinks = streamGraph
				.getSinkIDs()
				.stream()
				.map(streamGraph::getStreamNode)
				.collect(Collectors.toList());

		addSinkEntities(sinks, flinkApp, entity);
	}

	private void addSourceEntities(List<StreamNode> sources, AtlasEntity flinkApp, AtlasEntity.AtlasEntityExtInfo entityExtInfo) {
		List<AtlasEntity> inputs = new ArrayList<>();

		for (StreamNode source : sources) {
			StreamSource<?, ?> sourceOperator = (StreamSource<?, ?>) source.getOperator();
			SourceFunction<?> sourceFunction = sourceOperator.getUserFunction();

			List<AtlasEntity> dsEntities = createSourceEntity(sourceFunction, entityExtInfo);
			inputs.addAll(dsEntities);
		}

		flinkApp.setRelationshipAttribute("inputs", AtlasTypeUtil.getAtlasRelatedObjectIds(inputs, RELATIONSHIP_DATASET_PROCESS_INPUTS));
	}

	private List<AtlasEntity> createSourceEntity(SourceFunction<?> sourceFunction, AtlasEntity.AtlasEntityExtInfo entityExtInfo) {

		List<AtlasEntity> ret = new ArrayList<>();

		String sourceClass = sourceFunction.getClass().getName();

		if (sourceClass.equals("org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer")) {
			KafkaEntities.addKafkaSourceEntity((FlinkKafkaConsumer<?>) sourceFunction, ret, getMetadataNamespace());
		} else if (sourceClass.equals("org.apache.flink.streaming.api.functions.source.ContinuousFileMonitoringFunction")) {
			FileEntities.addMonitorSourceEntity((ContinuousFileMonitoringFunction<?>) sourceFunction, ret, getMetadataNamespace());
		}

		ret.forEach(entityExtInfo::addReferredEntity);
		return ret;
	}

	private void addSinkEntities(List<StreamNode> sinks, AtlasEntity flinkApp, AtlasEntity.AtlasEntityExtInfo entityExtInfo) {
		List<AtlasEntity> outputs = new ArrayList<>();
		for (StreamNode sink : sinks) {
			StreamSink<?> sinkOperator = (StreamSink<?>) sink.getOperator();
			SinkFunction<?> sinkFunction = sinkOperator.getUserFunction();

			List<AtlasEntity> dsEntities = createSinkEntity(sinkFunction, entityExtInfo);
			outputs.addAll(dsEntities);

		}

		flinkApp.setRelationshipAttribute("outputs", AtlasTypeUtil.getAtlasRelatedObjectIds(outputs, RELATIONSHIP_PROCESS_DATASET_OUTPUTS));
	}

	private List<AtlasEntity> createSinkEntity(SinkFunction<?> sinkFunction, AtlasEntity.AtlasEntityExtInfo entityExtInfo) {

		List<AtlasEntity> ret = new ArrayList<>();

		String sinkClass = sinkFunction.getClass().getName();

		if (sinkClass.equals("org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer")) {
			KafkaEntities.addKafkaSinkEntity((FlinkKafkaProducer<?>) sinkFunction, ret, getMetadataNamespace());
		} else if (sinkClass.equals("org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink")) {
			FileEntities.addStreamFileSinkEntity((StreamingFileSink<?>) sinkFunction, ret, getMetadataNamespace());
		}

		ret.forEach(e -> e.setAttribute(AtlasClient.OWNER, getUser()));
		ret.forEach(entityExtInfo::addReferredEntity);
		return ret;
	}

}
