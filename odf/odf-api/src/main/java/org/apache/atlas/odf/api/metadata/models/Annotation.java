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
package org.apache.atlas.odf.api.metadata.models;

// JSON
/**
 * This class represents a result of a discovery service analysis on a single metadata object.
 * By extending this class new annotation types for new discovery services can be created in order to provide additional information
 *
 */
public abstract class Annotation extends MetaDataObject {
	
	private String annotationType = this.getClass().getSimpleName().replace('$', '_');
	private String analysisRun;
	private String jsonProperties;
	private String summary;

	public String getAnnotationType() {
		return annotationType;
	}

	public void setAnnotationType(String annotationType) {
		this.annotationType = annotationType;
	}

	public String getJsonProperties() {
		return jsonProperties;
	}

	public void setJsonProperties(String jsonProperties) {
		this.jsonProperties = jsonProperties;
	}

	public String getAnalysisRun() {
		return analysisRun;
	}

	public void setAnalysisRun(String analysisRun) {
		this.analysisRun = analysisRun;
	}

	public String getSummary() {
		return summary;
	}

	public void setSummary(String summary) {
		this.summary = summary;
	}
	
}
