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

import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.source.ContinuousFileMonitoringFunction;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasConstants;
import org.apache.atlas.flink.model.FlinkDataTypes;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.utils.HdfsNameServiceResolver;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;

import java.util.List;

public class FileEntities {

	public static void addStreamFileSinkEntity(StreamingFileSink fileSink, List<AtlasEntity> ret, String metadataNamespace) {
		addFileEntity(new Path(fileSink.getBucketsBuilder().getBasePath().toUri()), ret, metadataNamespace);

	}

	public static void addMonitorSourceEntity(ContinuousFileMonitoringFunction monitorSource, List<AtlasEntity> ret, String metadataNamespace) {
		addFileEntity(new Path(monitorSource.getMonitoredPath()), ret, metadataNamespace);
	}

	private static void addFileEntity(Path hdfsPath, List<AtlasEntity> ret, String metadataNamespace) {
		final String nameServiceID = HdfsNameServiceResolver.getNameServiceIDForPath(hdfsPath.toString());

		AtlasEntity e = new AtlasEntity(FlinkDataTypes.HDFS_PATH.getName());

		e.setAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, metadataNamespace);
		e.setAttribute(AtlasClient.NAME, Path.getPathWithoutSchemeAndAuthority(hdfsPath).toString().toLowerCase());

		if (StringUtils.isNotEmpty(nameServiceID)) {
			String updatedPath = HdfsNameServiceResolver.getPathWithNameServiceID(hdfsPath.toString());

			e.setAttribute("path", updatedPath);
			e.setAttribute("nameServiceId", nameServiceID);
			e.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, getHdfsPathQualifiedName(metadataNamespace, updatedPath));
		} else {
			e.setAttribute("path", hdfsPath.toString());
			e.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, getHdfsPathQualifiedName(metadataNamespace, hdfsPath.toString()));
		}
		ret.add(e);
	}

	private static String getHdfsPathQualifiedName(String metadataNamespace, String hdfsPath) {
		return String.format("%s@%s", hdfsPath.toLowerCase(), metadataNamespace);
	}
}
