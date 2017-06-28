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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.metadata.models.Annotation;
import org.apache.wink.json4j.JSONException;
import org.apache.wink.json4j.JSONObject;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

/**
 * The Jackson serializer for Annotation objects
 * 
 *
 */
public class AnnotationSerializer extends StdSerializer<Annotation> {

	public AnnotationSerializer() {
		this(null);
	}
	
	public AnnotationSerializer(Class<Annotation> t) {
		super(t);
	}
	
	Logger logger = Logger.getLogger(AnnotationSerializer.class.getName());

	ClassLoader getClassLoader() {
		return this.getClass().getClassLoader();
	}
	
	// In the following jsonProperties is either already pre-populated (because we are serializing an instance of ProfilingAnnotation, ....
	// or it is created from all attributes not present in ProfilingAnnotation, or its ancestors (e.g. serializing an instance of ColumnAnalysisColumnAnntation)
	// in the latter case jsonProperties is expected to be null
	
	@Override
	public void serialize(Annotation annot, JsonGenerator jg, SerializerProvider sp) throws IOException, JsonProcessingException {
		jg.writeStartObject();
		Class<?> cl = annot.getClass();
		class JSONPropField {
			String name;
			Object value;
			JSONPropField(String name, Object value) {this.name = name; this.value = value;}
		}
		ArrayList<JSONPropField> jsonPropFields = null;
		String jsonPropertiesValue = null;
		while (cl != Object.class) {   // process class hierarchy up to and including MetaDataObject.class
			Field fields[] = cl.getDeclaredFields();
			for (Field f: fields) {
				f.setAccessible(true);
				String fieldName = f.getName();
				try {
					Object fieldValue = f.get(annot);
					if (fieldName.equals("jsonProperties")) {
						jsonPropertiesValue = (String)fieldValue;
					}
					else if (JSONUtils.annotationFields.contains(fieldName)) {
						jg.writeFieldName(fieldName);
						jg.writeObject(fieldValue);							
					}
					else {
						if (jsonPropFields == null) jsonPropFields = new ArrayList<JSONPropField>();
						jsonPropFields.add(new JSONPropField(fieldName, fieldValue));
					}
				}
				catch (IllegalAccessException e) {
					throw new IOException(e);
				}
			}
			cl = cl.getSuperclass();
		}
		jg.writeFieldName("jsonProperties");
		if (jsonPropFields != null) {
			jg.writeStartObject();
			if (jsonPropertiesValue != null) {
				try {
					JSONObject jo = new JSONObject(jsonPropertiesValue);
					Iterator<String> it = jo.keys();
			         while(it.hasNext()) {
			             String key = it.next();
			             jg.writeFieldName(key);
			             jg.writeObject(jo.get(key));
					}					
				}
				catch (JSONException e) {
					throw new IOException(e);					
				}
			}
			for (JSONPropField jpf:jsonPropFields) {
				jg.writeFieldName(jpf.name);
				jg.writeObject(jpf.value);								
			}
			jg.writeEndObject();				
		}
		else {
			jg.writeString(jsonPropertiesValue);
		}
		jg.writeEndObject();
	}

}
