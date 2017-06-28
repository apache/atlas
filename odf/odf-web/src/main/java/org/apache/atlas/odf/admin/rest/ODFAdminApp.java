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
package org.apache.atlas.odf.admin.rest;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.core.Application;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import org.apache.atlas.odf.admin.rest.resources.AnalysesResource;
import org.apache.atlas.odf.admin.rest.resources.AnnotationsResource;
import org.apache.atlas.odf.admin.rest.resources.DiscoveryServicesResource;
import org.apache.atlas.odf.admin.rest.resources.EngineResource;
import org.apache.atlas.odf.admin.rest.resources.ImportResource;
import org.apache.atlas.odf.admin.rest.resources.MetadataResource;
import org.apache.atlas.odf.admin.rest.resources.SettingsResource;

public class ODFAdminApp extends Application {
	@Override
	public Set<Class<?>> getClasses() {
		Set<Class<?>> classes = new HashSet<Class<?>>();
		classes.add(AnalysesResource.class);
		classes.add(SettingsResource.class);
		classes.add(EngineResource.class);
		classes.add(MetadataResource.class);
		classes.add(AnnotationsResource.class);
		classes.add(DiscoveryServicesResource.class);
		classes.add(ImportResource.class);
		return classes;
	}

	@Override
	public Set<Object> getSingletons() {
		Set<Object> set = new HashSet<Object>();
		set.add(new JacksonJsonProvider());
		return set;
	}
}
