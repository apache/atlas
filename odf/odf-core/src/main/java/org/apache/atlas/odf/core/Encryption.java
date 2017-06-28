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
package org.apache.atlas.odf.core;

import org.jasypt.exceptions.EncryptionOperationNotPossibleException;
import org.jasypt.util.text.BasicTextEncryptor;
import org.apache.commons.codec.binary.Base64;

public class Encryption {

	//TODO Store this password at a secure location provided by the surrounding platform.
	private static final String b64EncryptionPassword = "eGg1NyQyMyUtIXFQbHoxOHNIdkM=";

	public static String encryptText(String plainText) {
		if ((plainText != null) && (!plainText.isEmpty())) {
			BasicTextEncryptor textEncryptor = new BasicTextEncryptor();
			byte[] plainEncryptionPassword= Base64.decodeBase64(b64EncryptionPassword);
			textEncryptor.setPassword(new String(plainEncryptionPassword));
			return textEncryptor.encrypt(plainText);
		} else {
			return plainText;
		}
	}

	public static String decryptText(String encryptedText) {
		if ((encryptedText != null) && (!encryptedText.isEmpty())) {
			BasicTextEncryptor textEncryptor = new BasicTextEncryptor();
			byte[] plainEncryptionPassword= Base64.decodeBase64(b64EncryptionPassword);
			textEncryptor.setPassword(new String(plainEncryptionPassword));
			String result = textEncryptor.decrypt(encryptedText);
			return result;
		} else {
			return encryptedText;
		}
	}
	
	public static boolean isEncrypted(String text) {
		try {
			decryptText(text);
		} catch(EncryptionOperationNotPossibleException exc) {
			return false;
		}
		return true;
	}

	/*
	// Uncomment and use the following code for encrypting passwords to be stored in the odf-initial-configuration.json file.
	public static void main(String[] args) {
		if (args.length != 1)  {
			System.out.println("usage: java Encryption <plain password>");
		} else {
			System.out.println("Encrypted password: " + encryptText(args[0]));
		}
	}
	 */
}
