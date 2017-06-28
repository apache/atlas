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

//JSON
/**
 * 
 * This class describes a REST endpoint representing a remote service that can be used by ODF
 * Note: It doesn't inherit from DiscoveryServiceEndpoint. To convert this from / to this class use JSONUtils.convert()
 *
 */
public class DiscoveryServiceSparkEndpoint {
	/**
	 * This property informs ODF about the type of input for the underlying Spark job, (CSV) file vs. (Database) connection.  
	 */
	public static enum SERVICE_INTERFACE_TYPE {
		DataFrame, Generic
	}

	public static String ANNOTATION_PROPERTY_COLUMN_NAME = "ODF_ANNOTATED_COLUMN";
	public static String ANNOTATION_SUMMARY_COLUMN_NAME = "ODF_ANNOTATION_SUMMARY";
	public static String ODF_BEGIN_OF_ANNOTATION_RESULTS = "***ODF_BEGIN_OF_ANNOTATION_RESULTS***\n";

	private String runtimeName;

	private SERVICE_INTERFACE_TYPE inputMethod = null;

	private String jar;

	private String className;

	public DiscoveryServiceSparkEndpoint() {
		this.setRuntimeName("Spark");
	}
	
	public String getJar() {
		return jar;
	}

	public void setJar(String jar) {
		this.jar = jar;
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public SERVICE_INTERFACE_TYPE getInputMethod() {
		return inputMethod;
	}

	public void setInputMethod(SERVICE_INTERFACE_TYPE inputMethod) {
		this.inputMethod = inputMethod;
	}

	public String getRuntimeName() {
		return runtimeName;
	}

	public void setRuntimeName(String runtimeName) {
		this.runtimeName = runtimeName;
	}

}
