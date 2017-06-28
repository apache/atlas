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
import java.lang.reflect.Field;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.atlas.odf.api.metadata.models.Annotation;
import org.apache.atlas.odf.api.metadata.models.ClassificationAnnotation;
import org.apache.atlas.odf.api.metadata.models.ProfilingAnnotation;
import org.apache.atlas.odf.api.metadata.models.RelationshipAnnotation;

/**
 * The Jackson deserializer for Annotation objects
 * 
 *
 */
public class AnnotationDeserializer extends StdDeserializer<Annotation> {

	private static final long serialVersionUID = -3143233438847937374L;
	
	Logger logger = Logger.getLogger(AnnotationDeserializer.class.getName());
	
	public AnnotationDeserializer() {
		super(Annotation.class);
	}

	ClassLoader getClassLoader() {
		return this.getClass().getClassLoader();
	}
	
	@Override
	public Annotation deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
		ObjectMapper jpom = ((ObjectMapper) jp.getCodec());
		ObjectNode tree = jpom.readTree(jp);
		String jsonString = tree.toString();
		Annotation result = null;

		Class<? extends Annotation> javaClass = null;
		JsonNode javaClassNode = tree.get("javaClass");
		if (javaClassNode == null) {
			throw new IOException("Can not deserialize object since the javaClass attribute is missing: " + jsonString);
		}
		JsonNode jsonPropertiesNode = tree.get("jsonProperties");
		String javaClassName = javaClassNode.asText();
		if (javaClassName.equals(ProfilingAnnotation.class.getName())) {
			javaClass = ProfilingAnnotation.class;
		}
		else if (javaClassName.equals(ClassificationAnnotation.class.getName())) {
			javaClass = ClassificationAnnotation.class;
		}
		else if (javaClassName.equals(RelationshipAnnotation.class.getName())) {
			javaClass = RelationshipAnnotation.class;
		}
		else {
			try {
				javaClass = (Class<? extends Annotation>) this.getClassLoader().loadClass(javaClassName);
				if (jsonPropertiesNode != null && !jsonPropertiesNode.isNull()) { // unfold jsonProperties in case of specific annotations
					JsonNode jsonPropertiesNodeUnfolded = null;
					if (jsonPropertiesNode.isTextual()) {
						jsonPropertiesNodeUnfolded = jpom.readTree(jsonPropertiesNode.asText());					
					}
					else {
						jsonPropertiesNodeUnfolded = jsonPropertiesNode; 
					}
					JsonNode newJsonPropertiesNode = (JsonNode)jp.getCodec().createObjectNode();    // initialize new jsonProperties node
					Field classFields[] = javaClass.getDeclaredFields();
					HashSet<String> classFieldSet = new HashSet<String>();
					for (Field f: classFields) {
						f.setAccessible(true);
						String fieldName = f.getName();
						classFieldSet.add(fieldName);
					}
					Iterator<Entry<String,JsonNode>> jsonPropertiesFields = jsonPropertiesNodeUnfolded.fields();
					while (jsonPropertiesFields.hasNext()) { 
						Entry<String,JsonNode> field = jsonPropertiesFields.next();
						String fieldName = field.getKey();
						if (JSONUtils.annotationFields.contains(fieldName)) {
							throw new IOException("Name conflict: Field name in jsonProperties matches predefined field [" + fieldName + "]");
						}
						JsonNode fieldValue = field.getValue();
						if (classFieldSet.contains(fieldName)) {
							tree.set(fieldName, fieldValue);							
						}
						else {
							((ObjectNode)newJsonPropertiesNode).set(fieldName, field.getValue());							
						}
					}
					tree.put("jsonProperties", newJsonPropertiesNode.textValue());
				}
			} catch (ClassNotFoundException exc) {
				String msg = MessageFormat.format("Java class ''{0}'' could not be deserialized automatically (probably because it is not on the classpath)", javaClassName);
				logger.warning(msg);
				logger.log(Level.FINE, msg, exc);
			}
			if (javaClass == null) {
				if (tree.get("profiledObject") != null) {   // class not found -> create as instance of corresponding 'unknown' types
					javaClass = ProfilingAnnotation.class;
				}
				else if (tree.get("classifiedObject") != null) {
					javaClass = ClassificationAnnotation.class;
				}
				else if (tree.get("relatedObjects") != null) {
					javaClass = RelationshipAnnotation.class;
				}
				else { // malformed annotation
					javaClass = Annotation.class;
				}
				if (jsonPropertiesNode == null) {
					jsonPropertiesNode = (JsonNode)jp.getCodec().createObjectNode(); // initialize if not already present
				}
				Iterator<Entry<String,JsonNode>> fields = tree.fields();
				ArrayList<String> fieldsToRemove = new ArrayList<String>();
				try {
					while (fields.hasNext()) {     // move all fields not present in the predefined annotation types
						Entry<String,JsonNode> field = fields.next();   // to the string valued jsonProperties attribute
						String fieldName = field.getKey();
						if (!JSONUtils.annotationFields.contains(fieldName)) {
							((ObjectNode)jsonPropertiesNode).set(fieldName, field.getValue());
							fieldsToRemove.add(fieldName);
						}
					}
					String jsonProperties = (jsonPropertiesNode.isTextual()) ? jsonPropertiesNode.textValue() : jsonPropertiesNode.toString();
					tree.put("jsonProperties", jsonProperties); 
					for (String fieldToRemove:fieldsToRemove) {  // remove fields not present in the predefined annotation types
						tree.remove(fieldToRemove);
					}
				}
				catch (Exception e) {
					throw new IOException(e);
				}
			}
			jsonString = tree.toString();				
		}
		result = jpom.readValue(jsonString, javaClass);
		logger.log(Level.FINEST, "Annotation created. Original: {0}, deserialized annotation: {1}", new Object[]{ jsonString, JSONUtils.lazyJSONSerializer(result)});
		return result;
	}

}
