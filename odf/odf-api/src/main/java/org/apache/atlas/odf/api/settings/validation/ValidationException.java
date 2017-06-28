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

public class ValidationException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 485240669635915916L;
	private String property;
	private String errorCause;

	public ValidationException(String property, String errorMessage) {
		this.errorCause = errorMessage;
		this.property = property;
	}

	public ValidationException(String errorCause) {
		this.errorCause = errorCause;
	}

	@Override
	public String getMessage() {
		if (property != null && errorCause != null) {
			return MessageFormat.format("Error setting property {0}, {1}", property, errorCause);
		} else if (errorCause != null) {
			return MessageFormat.format("Error setting property value, {0}", errorCause);
		} else {
			return "Error setting property value.";
		}
	}

	public String getProperty() {
		return property;
	}

	public void setProperty(String property) {
		this.property = property;
	}

	public String getErrorCause() {
		return errorCause;
	}

	public void setErrorCause(String error) {
		this.errorCause = error;
	}

}
