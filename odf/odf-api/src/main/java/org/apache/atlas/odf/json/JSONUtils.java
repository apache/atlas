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
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.wink.json4j.JSONArray;
import org.apache.wink.json4j.JSONException;
import org.apache.wink.json4j.JSONObject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.atlas.odf.api.metadata.UnknownMetaDataObject;
import org.apache.atlas.odf.api.metadata.models.Annotation;
import org.apache.atlas.odf.api.metadata.models.ClassificationAnnotation;
import org.apache.atlas.odf.api.metadata.models.Connection;
import org.apache.atlas.odf.api.metadata.models.ConnectionInfo;
import org.apache.atlas.odf.api.metadata.models.DataSet;
import org.apache.atlas.odf.api.metadata.models.DataStore;
import org.apache.atlas.odf.api.metadata.models.MetaDataObject;
import org.apache.atlas.odf.api.metadata.models.ProfilingAnnotation;
import org.apache.atlas.odf.api.metadata.models.RelationshipAnnotation;
import org.apache.atlas.odf.api.metadata.models.UnknownDataSet;
import org.apache.atlas.odf.api.metadata.models.UnknownConnection;
import org.apache.atlas.odf.api.metadata.models.UnknownConnectionInfo;
import org.apache.atlas.odf.api.metadata.models.UnknownDataStore;

public class JSONUtils {
	
	public static HashSet<String> annotationFields = new HashSet<String>();
	
	static {
		for (Class<?> cl: new Class<?>[]{Annotation.class, ProfilingAnnotation.class, ClassificationAnnotation.class,RelationshipAnnotation.class}) {
			while (cl != Object.class) {   // process class hierarchy up to and including MetaDataObject.class
				Field fields[] = cl.getDeclaredFields();
				for (Field f: fields) {
					f.setAccessible(true);
					annotationFields.add(f.getName());
				}
				cl = cl.getSuperclass();
			}			
		}
	}



	// reuse object mapper for performance
	private static ObjectMapper om = null;

	static {
		om = new ObjectMapper();
		Module mod = createDefaultObjectMapperModule();
		om.registerModule(mod);
	}

	public static ObjectMapper getGlobalObjectMapper() {
		return om;
	}

	static Module createDefaultObjectMapperModule() {
		SimpleModule mod = new SimpleModule("ODF Jackson module", Version.unknownVersion());
		mod.addDeserializer(Annotation.class, new AnnotationDeserializer());
		mod.addDeserializer(MetaDataObject.class, new DefaultODFDeserializer<MetaDataObject>(MetaDataObject.class, UnknownMetaDataObject.class));
		mod.addDeserializer(DataSet.class, new DefaultODFDeserializer<DataSet>(DataSet.class, UnknownDataSet.class));
		mod.addDeserializer(DataStore.class, new DefaultODFDeserializer<DataStore>(DataStore.class, UnknownDataStore.class));
		mod.addDeserializer(Connection.class, new DefaultODFDeserializer<Connection>(Connection.class, UnknownConnection.class));
		mod.addDeserializer(ConnectionInfo.class, new DefaultODFDeserializer<ConnectionInfo>(ConnectionInfo.class, UnknownConnectionInfo.class));
		
		mod.addSerializer(Annotation.class, new AnnotationSerializer());
		return mod;

	}
	
	public static JSONObject toJSONObject(Object o) throws JSONException {
		JSONObject result;
		try {
			result = new JSONObject(om.writeValueAsString(o));
			if (o instanceof Annotation) {
				Object jsonPropsObject = result.get("jsonProperties");
				if (jsonPropsObject instanceof JSONObject) {    // the value of jsonProperties must be of type 'String'
					result.put("jsonProperties", ((JSONObject)jsonPropsObject).toString());	
				}
			}
		} catch (JsonProcessingException e) {
			throw new JSONException(e);
		}
		return result;
	}

	public static String toJSON(Object o) throws JSONException {
		String result;
		try {
			result = om.writeValueAsString(o);
			if (o instanceof Annotation) {
				JSONObject json = new JSONObject(result);
				Object jsonPropsObject = json.get("jsonProperties");
				if (jsonPropsObject instanceof JSONObject) {    // the value of jsonProperties must be of type 'String'
					json.put("jsonProperties", ((JSONObject)jsonPropsObject).toString());	
					result = json.toString();
				}
			}
		} catch (JsonProcessingException e) {
			throw new JSONException(e);
		}
		return result;
	}

	public static <T> List<T> fromJSONList(String s, Class<T> cl) throws JSONException {
		JSONArray ar = new JSONArray(s);
		List<T> result = new ArrayList<>();
		for (Object o : ar) {
			JSONObject jo = (JSONObject) o;
			T t = (T) fromJSON(jo.write(), cl);
			result.add(t);
		}
		return result;

	}

	public static <T> List<T> fromJSONList(InputStream is, Class<T> cl) throws JSONException {
		JSONArray ar = new JSONArray(is);
		List<T> result = new ArrayList<>();
		for (Object o : ar) {
			JSONObject jo = (JSONObject) o;
			T t = (T) fromJSON(jo.write(), cl);
			result.add(t);
		}
		return result;
	}

	public static <T> T fromJSON(String s, Class<T> cl) throws JSONException {
		T result = null;
		try {
			result = om.readValue(s, cl);
		} catch (JsonProcessingException exc) {
			// propagate JSON exception
			throw new JSONException(exc);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		return result;
	}

	public static <T> T fromJSON(InputStream is, Class<T> cl) throws JSONException {
		return fromJSON(getInputStreamAsString(is, "UTF-8"), cl);
	}

	public static <T> T readJSONObjectFromFileInClasspath(Class<T> cl, String pathToFile, ClassLoader classLoader) {
		if (classLoader == null) {
			// use current classloader if not provided
			classLoader = JSONUtils.class.getClassLoader();
		}
		InputStream is = classLoader.getResourceAsStream(pathToFile);
		T result = null;
		try {
			result = om.readValue(is, cl);
		} catch (IOException e) {
			// assume that this is a severe error since the provided JSONs should be correct
			throw new RuntimeException(e);
		}

		return result;
	}

	public static <T> T cloneJSONObject(T obj) throws JSONException {
		// special case: use Annotation.class in case obj is an annotation subclass to ensure that the annotation deserializer is used
		if (Annotation.class.isAssignableFrom(obj.getClass())) {
			return (T) fromJSON(toJSON(obj), Annotation.class);
		}
		return fromJSON(toJSON(obj), (Class<T>) obj.getClass());
	}

	
	public static void mergeJSONObjects(JSONObject source, JSONObject target) {
		if (source != null && target != null) {
			target.putAll(source);
		}
	}

	// use this method, e.g., if you want to use JSON objects in log / trace messages
	// and want to do serialization only if tracing is on
	public static Object lazyJSONSerializer(final Object jacksonObject) {
		return new Object() {

			@Override
			public String toString() {
				try {
					return toJSON(jacksonObject);
				} catch (JSONException e) {
					return e.getMessage();
				}
			}

		};
	}

	public static Object jsonObject4Log(final JSONObject obj) {
		return new Object() {

			@Override
			public String toString() {
				try {
					return obj.write();
				} catch (Exception e) {
					return e.getMessage();
				}
			}

		};
	}

	public static String getInputStreamAsString(InputStream is, String encoding) {
		try {
			final int n = 2048;
			byte[] b = new byte[0];
			byte[] temp = new byte[n];
			int bytesRead;
			while ((bytesRead = is.read(temp)) != -1) {
				byte[] newB = new byte[b.length + bytesRead];
				System.arraycopy(b, 0, newB, 0, b.length);
				System.arraycopy(temp, 0, newB, b.length, bytesRead);
				b = newB;
			}
			String s = new String(b, encoding);
			return s;
		} catch (IOException exc) {
			throw new RuntimeException(exc);
		}
	}
	
	public static <T, S> T convert(S source, Class<T> targetClass) throws JSONException {
		return fromJSON(toJSON(source), targetClass);
	}
}
