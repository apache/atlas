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

package org.apache.atlas.plugin.store;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.atlas.plugin.model.RangerServiceDef;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

public class ServiceDefsUtil {
	private static final Log LOG = LogFactory.getLog(ServiceDefsUtil.class);

	public static final String EMBEDDED_SERVICEDEF_TAG_NAME  = "tag";

	private static ServiceDefsUtil instance = new ServiceDefsUtil();

	private final Gson              gsonBuilder;

	/** Private constructor to restrict instantiation of this singleton utility class. */
	private ServiceDefsUtil() {
		gsonBuilder = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").setPrettyPrinting().create();
	}

	public static ServiceDefsUtil instance() {
		return instance;
	}

	public RangerServiceDef getEmbeddedServiceDef(String defType) throws Exception {
		RangerServiceDef serviceDef=null;
		if(StringUtils.isNotEmpty(defType)){
			serviceDef=loadEmbeddedServiceDef(defType);
		}
		return serviceDef;
	}

	public static boolean isRecursiveEnabled(final RangerServiceDef rangerServiceDef, final String resourceDefName) {
		boolean ret = false;
		List<RangerServiceDef.RangerResourceDef>  resourceDefs = rangerServiceDef.getResources();
		for(RangerServiceDef.RangerResourceDef resourceDef:resourceDefs) {
			if (resourceDefName.equals(resourceDef.getName())) {
				ret =  resourceDef.getRecursiveSupported();
				break;
			}
		}
		return ret;
	}

	private RangerServiceDef loadEmbeddedServiceDef(String serviceType) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> EmbeddedServiceDefsUtil.loadEmbeddedServiceDef(" + serviceType + ")");
		}

		RangerServiceDef ret = null;

		String resource = "/service-defs/atlas-servicedef-" + serviceType + ".json";

		try (InputStream inStream = getClass().getResourceAsStream(resource)) {

			try (InputStreamReader reader = new InputStreamReader(inStream)) {
				ret = gsonBuilder.fromJson(reader, RangerServiceDef.class);

				//Set DEFAULT displayName if missing
				if (ret != null && StringUtils.isBlank(ret.getDisplayName())) {
					ret.setDisplayName(ret.getName());
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> EmbeddedServiceDefsUtil.loadEmbeddedServiceDef(" + serviceType + ")");
		}

		return ret;
	}
}
