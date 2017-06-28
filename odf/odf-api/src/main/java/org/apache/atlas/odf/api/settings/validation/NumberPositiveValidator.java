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
package org.apache.atlas.odf.api.settings.validation;

public class NumberPositiveValidator implements PropertyValidator {

	public void validate(String property, Object value) throws ValidationException {
		if (!(value instanceof Number)) {
			throw new ValidationException("Only numbers are allowed!");
		} else {
			if (value instanceof Long && (long) value < 0) {
				throw new ValidationException(property, "Only positive values are allowed!");
			} else if (value instanceof Integer && (int) value < 0) {
				throw new ValidationException(property, "Only positive values are allowed!");
			} else if (value instanceof Double && (double) value < 0) {
				throw new ValidationException(property, "Only positive values are allowed!");
			}
		}
	}

}
