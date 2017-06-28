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
package org.apache.atlas.odf.json;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

public class DefaultODFDeserializer<T> extends StdDeserializer<T> {
	private static final long serialVersionUID = 4895771352050172936L;

	Logger logger = Logger.getLogger(DefaultODFDeserializer.class.getName());

	Class<? extends T> defaultClass;

	public DefaultODFDeserializer(Class<T> cl, Class<? extends T> defaultClass) {
		super(cl);
		this.defaultClass = defaultClass;
	}

	ClassLoader getClassLoader() {
		return this.getClass().getClassLoader();
	}

	@Override
	public T deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
		ObjectMapper jpom = ((ObjectMapper) jp.getCodec());
		JsonNode tree = jpom.readTree(jp);
		String jsonString = tree.toString();

		Class<? extends T> javaClass = null;
		String javaClassName = null;
		try {
			JsonNode javaClassNode = tree.get("javaClass");
			javaClassName = javaClassNode.asText();
			logger.log(Level.FINEST, "Trying to deserialize object of java class {0}", javaClassName);
			javaClass = (Class<? extends T>) this.getClassLoader().loadClass(javaClassName);
			if (javaClass != null) {
				if (!javaClass.equals(this.handledType())) {
					return jpom.readValue(jsonString, javaClass);
				}
			}
		} catch (Exception exc) {
			String msg = MessageFormat.format("Java class ''{0}'' could not be deserialized automatically (probably because it is not on the classpath)", javaClassName);
			logger.warning(msg);
			logger.log(Level.FINE, msg, exc);
		}
		return jpom.readValue(jsonString, defaultClass);
	}
}
