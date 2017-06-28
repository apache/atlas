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
package org.apache.atlas.odf.core.messaging;

/**
 * Default encryption: no encryption
 * 
 */
public class DefaultMessageEncryption implements MessageEncryption {
	
	@Override
	public String encrypt(String message) {
		return message;
	}

	@Override
	public String decrypt(String message) {
		return message;
	}


	/*
	// this used to be our default encryption. Leaving it in here for reference.
	@Override
	public String encrypt(String message) {
		try {
			return DatatypeConverter.printBase64Binary(message.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String decrypt(String message)  {
		try {
			return new String(DatatypeConverter.parseBase64Binary(message), "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}
	*/
}
