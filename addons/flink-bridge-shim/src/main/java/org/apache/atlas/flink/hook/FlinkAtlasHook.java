
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
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;

import org.apache.atlas.plugin.classloader.AtlasPluginClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Flink hook used for atlas entity registration.
 */
public class FlinkAtlasHook implements JobListener {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkAtlasHook.class);

	private static final String ATLAS_PLUGIN_TYPE = "flink";
	private static final String ATLAS_FLINK_HOOK_IMPL_CLASSNAME = "org.apache.atlas.flink.hook.FlinkAtlasHook";

	private AtlasPluginClassLoader atlasPluginClassLoader = null;
	private JobListener flinkHook = null;

	public FlinkAtlasHook() {
		this.initialize();
	}

	@Override
	public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
		LOG.debug("==> FlinkAtlasHook.onJobSubmitted");

		try {
			activatePluginClassLoader();
			flinkHook.onJobSubmitted(jobClient, throwable);
		} finally {
			deactivatePluginClassLoader();
		}

		LOG.debug("<== FlinkAtlasHook.onJobSubmitted");
	}

	@Override
	public void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {
		LOG.debug("==> FlinkAtlasHook.onJobExecuted");

		try {
			activatePluginClassLoader();
			flinkHook.onJobExecuted(jobExecutionResult, throwable);
		} finally {
			deactivatePluginClassLoader();
		}

		LOG.debug("<== FlinkAtlasHook.onJobExecuted");
	}

	private void initialize() {
		LOG.debug("==> FlinkAtlasHook.initialize()");

		try {
			atlasPluginClassLoader = AtlasPluginClassLoader.getInstance(ATLAS_PLUGIN_TYPE, this.getClass());

			@SuppressWarnings("unchecked")
			Class<JobListener> cls = (Class<JobListener>) Class
					.forName(ATLAS_FLINK_HOOK_IMPL_CLASSNAME, true, atlasPluginClassLoader);

			activatePluginClassLoader();

			flinkHook = cls.newInstance();
		} catch (Exception excp) {
			LOG.error("Error instantiating Atlas hook implementation", excp);
		} finally {
			deactivatePluginClassLoader();
		}

		LOG.debug("<== FlinkAtlasHook.initialize()");
	}

	private void activatePluginClassLoader() {
		if (atlasPluginClassLoader != null) {
			atlasPluginClassLoader.activate();
		}
	}

	private void deactivatePluginClassLoader() {
		if (atlasPluginClassLoader != null) {
			atlasPluginClassLoader.deactivate();
		}
	}
}
