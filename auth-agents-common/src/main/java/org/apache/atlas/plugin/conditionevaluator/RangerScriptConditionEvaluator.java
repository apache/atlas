/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.atlas.plugin.conditionevaluator;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.atlas.plugin.classloader.RangerPluginClassLoader;
import org.apache.atlas.plugin.contextenricher.RangerTagForEval;
import org.apache.atlas.plugin.policyengine.RangerAccessRequest;
import org.apache.atlas.plugin.util.RangerPerfTracer;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.plugin.util.RangerCommonConstants.SCRIPT_OPTION_ENABLE_JSON_CTX;
import static org.apache.atlas.plugin.util.RangerCommonConstants.SCRIPT_VAR_CONTEXT;
import static org.apache.atlas.plugin.util.RangerCommonConstants.SCRIPT_VAR_CONTEXT_JSON;

public class RangerScriptConditionEvaluator extends RangerAbstractConditionEvaluator {
	private static final Log LOG = LogFactory.getLog(RangerScriptConditionEvaluator.class);

	private static final Log PERF_POLICY_CONDITION_SCRIPT_EVAL = RangerPerfTracer.getPerfLogger("policy.condition.script.eval");

	private static final String SCRIPT_PREEXEC = SCRIPT_VAR_CONTEXT + "=JSON.parse(" + SCRIPT_VAR_CONTEXT_JSON + ");";

	private ScriptEngine scriptEngine;
	private boolean      enableJsonCtx = false;

	@Override
	public void init() {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerScriptConditionEvaluator.init(" + condition + ")");
		}

		super.init();

		String engineName = "JavaScript";

		Map<String, String> evalOptions = conditionDef. getEvaluatorOptions();

		if (MapUtils.isNotEmpty(evalOptions)) {
			engineName = evalOptions.get("engineName");

			enableJsonCtx = Boolean.parseBoolean(evalOptions.getOrDefault(SCRIPT_OPTION_ENABLE_JSON_CTX, Boolean.toString(enableJsonCtx)));
		}

		if (StringUtils.isBlank(engineName)) {
			engineName = "JavaScript";
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("RangerScriptConditionEvaluator.init() - engineName=" + engineName);
		}

		String conditionType = condition != null ? condition.getType() : null;

		try {
			ScriptEngineManager manager = new ScriptEngineManager();

			if (LOG.isDebugEnabled()) {
				List<ScriptEngineFactory> factories = manager.getEngineFactories();

				if (CollectionUtils.isEmpty(factories)) {
					LOG.debug("List of scriptEngineFactories is empty!!");
				} else {
					for (ScriptEngineFactory factory : factories) {
						LOG.debug("engineName=" + factory.getEngineName() + ", language=" + factory.getLanguageName());
					}
				}
			}

			scriptEngine = manager.getEngineByName(engineName);
		} catch (Exception exp) {
			LOG.error("RangerScriptConditionEvaluator.init() failed with exception=" + exp);
		}

		if (scriptEngine == null) {
			LOG.warn("failed to initialize condition '" + conditionType + "': script engine '" + engineName + "' was not created in a default manner");
			LOG.info("Will try to get script-engine from plugin-class-loader");


			RangerPluginClassLoader pluginClassLoader;

			try {

				pluginClassLoader = RangerPluginClassLoader.getInstance(serviceDef.getName(), null);

				if (pluginClassLoader != null) {
					scriptEngine = pluginClassLoader.getScriptEngine(engineName);
				} else {
					LOG.error("Cannot get script-engine from null pluginClassLoader");
				}

			} catch (Throwable exp) {
				LOG.error("RangerScriptConditionEvaluator.init() failed with exception=", exp);
			}
		}

		if (scriptEngine == null) {
			LOG.error("failed to initialize condition '" + conditionType + "': script engine '" + engineName + "' was not created");
		} else {
			LOG.info("ScriptEngine for engineName=[" + engineName + "] is successfully created");
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerScriptConditionEvaluator.init(" + condition + ")");
		}
	}

	@Override
	public boolean isMatched(RangerAccessRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerScriptConditionEvaluator.isMatched()");
		}
		boolean result = true;

		if (scriptEngine != null) {

			String script = getScript();

			if (StringUtils.isNotBlank(script)) {

				RangerAccessRequest readOnlyRequest = request.getReadOnlyCopy();

				RangerScriptExecutionContext context    = new RangerScriptExecutionContext(readOnlyRequest);
				RangerTagForEval             currentTag = context.getCurrentTag();
				Map<String, String>          tagAttribs = currentTag != null ? currentTag.getAttributes() : Collections.emptyMap();

				Bindings bindings = scriptEngine.createBindings();

				bindings.put("ctx", context);
				bindings.put("tag", currentTag);
				bindings.put("tagAttr", tagAttribs);

				if (enableJsonCtx) {
					bindings.put(SCRIPT_VAR_CONTEXT_JSON, context.toJson());

					script = SCRIPT_PREEXEC + script;
				}

				if (LOG.isDebugEnabled()) {
					LOG.debug("RangerScriptConditionEvaluator.isMatched(): script={" + script + "}");
				}

				RangerPerfTracer perf = null;

				try {
					long requestHash = request.hashCode();

					if (RangerPerfTracer.isPerfTraceEnabled(PERF_POLICY_CONDITION_SCRIPT_EVAL)) {
						perf = RangerPerfTracer.getPerfTracer(PERF_POLICY_CONDITION_SCRIPT_EVAL, "RangerScriptConditionEvaluator.isMatched(requestHash=" + requestHash + ")");
					}

					Object ret = scriptEngine.eval(script, bindings);

					if (ret == null) {
						ret = context.getResult();
					}
					if (ret instanceof Boolean) {
						result = (Boolean) ret;
					}

				} catch (NullPointerException nullp) {
					LOG.error("RangerScriptConditionEvaluator.isMatched(): eval called with NULL argument(s)", nullp);

				} catch (ScriptException exception) {
					LOG.error("RangerScriptConditionEvaluator.isMatched(): failed to evaluate script," +
							" exception=" + exception);
				} finally {
					RangerPerfTracer.log(perf);
				}
			} else {
				String conditionType = condition != null ? condition.getType() : null;
				LOG.error("failed to evaluate condition '" + conditionType + "': script is empty");
			}

		} else {
			String conditionType = condition != null ? condition.getType() : null;
			LOG.error("failed to evaluate condition '" + conditionType + "': script engine not found");
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerScriptConditionEvaluator.isMatched(), result=" + result);
		}

		return result;

	}

	protected String getScript() {
		String ret = null;

		List<String> values = condition.getValues();

		if (CollectionUtils.isNotEmpty(values)) {

			String value = values.get(0);
			if (StringUtils.isNotBlank(value)) {
				ret = value.trim();
			}
		}

		return ret;
	}
}
