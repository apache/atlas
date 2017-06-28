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

import java.text.MessageFormat;

public class EnumValidator implements PropertyValidator {

	String[] validValues = new String[0];

	public EnumValidator(String... validValues) {
		this.validValues = validValues;
	}

	@Override
	public void validate(String property, Object value) throws ValidationException {
		for (String valid : validValues) {
			if (valid.equals(value)) {
				return;
			}
		}

		throw new ValidationException(property, MessageFormat.format("only the following values are allowed: ", validValues.toString()));
	}

}
