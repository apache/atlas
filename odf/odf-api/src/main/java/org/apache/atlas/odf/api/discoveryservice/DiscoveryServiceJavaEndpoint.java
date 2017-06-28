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
package org.apache.atlas.odf.api.discoveryservice;

/**
 * 
 * This class represents a java ODF discovery service endpoint. 
 * Note: It doesn't inherit from DiscoveryServiceEndpoint. To convert this from / to this class use JSONUtils.convert()
 * 
 */
public class DiscoveryServiceJavaEndpoint {

	private String runtimeName;
	/*
	 * The class name identifies a class that must be available on the classpath and implements the ODF service interface
	 */
	private String className;

	public DiscoveryServiceJavaEndpoint() {
		this.setRuntimeName("Java");
	}
	
	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public String getRuntimeName() {
		return runtimeName;
	}

	public void setRuntimeName(String runtimeName) {
		this.runtimeName = runtimeName;
	}

}
