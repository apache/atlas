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
package org.apache.atlas.odf.core.test.annotation;

import org.apache.atlas.odf.api.metadata.models.ProfilingAnnotation;

class ExtensionTestAnnotation extends ProfilingAnnotation {

	private String newStringProp1;
	private int newIntProp2;

	public String getNewStringProp1() {
		return newStringProp1;
	}

	public void setNewStringProp1(String newStringProp1) {
		this.newStringProp1 = newStringProp1;
	}

	public int getNewIntProp2() {
		return newIntProp2;
	}

	public void setNewIntProp2(int newIntProp2) {
		this.newIntProp2 = newIntProp2;
	}

}
