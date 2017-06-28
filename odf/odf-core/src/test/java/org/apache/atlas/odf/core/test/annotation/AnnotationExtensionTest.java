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

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.metadata.models.Annotation;
import org.apache.atlas.odf.api.metadata.models.ProfilingAnnotation;
import org.apache.atlas.odf.json.JSONUtils;
import org.apache.wink.json4j.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.atlas.odf.core.test.ODFTestLogger;
import org.apache.atlas.odf.core.test.TimerTestBase;
import org.apache.atlas.odf.json.AnnotationDeserializer;
import org.apache.atlas.odf.json.AnnotationSerializer;

public class AnnotationExtensionTest extends TimerTestBase {

	static Logger logger = ODFTestLogger.get();

	public static <T> T readJSONObjectFromFileInClasspath(ObjectMapper om, Class<T> cl, String pathToFile, ClassLoader classLoader) {
		if (classLoader == null) {
			// use current classloader if not provided
			classLoader = AnnotationExtensionTest.class.getClassLoader();
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

	@Test
	public void testWithUtils() throws Exception {
		testSimple(JSONUtils.getGlobalObjectMapper());
	}

	@Test
	public void testWithSeparateObjectMapper() throws Exception {
		ObjectMapper om = new ObjectMapper();
		SimpleModule mod = new SimpleModule("annotation module", Version.unknownVersion());
		mod.addDeserializer(Annotation.class, new AnnotationDeserializer());
		mod.addSerializer(Annotation.class, new AnnotationSerializer());
		om.registerModule(mod);
		testSimple(om);
	}

	private void testSimple(ObjectMapper om) throws Exception {
		ExtensionTestAnnotation newTestAnnot = new ExtensionTestAnnotation();
		String strValue = "newstring1";
		int intValue = 4237;
		newTestAnnot.setNewStringProp1(strValue);
		newTestAnnot.setNewIntProp2(intValue);
//		String newTestAnnotJSON = om.writeValueAsString(newTestAnnot);
		String newTestAnnotJSON = JSONUtils.toJSON(newTestAnnot).toString();
		logger.info("New test annot JSON: " + newTestAnnotJSON);

		logger.info("Deserializing with " + Annotation.class.getSimpleName() + "class as target class");
		Annotation annot1 = om.readValue(newTestAnnotJSON, Annotation.class);
		Assert.assertNotNull(annot1);
		logger.info("Deserialized annotation JSON (target: " + Annotation.class.getSimpleName() + "): " + om.writeValueAsString(annot1));
		logger.info("Deserialized annotation class (target: " + Annotation.class.getSimpleName() + "): " + annot1.getClass().getName());
		Assert.assertEquals(ExtensionTestAnnotation.class, annot1.getClass());
		ExtensionTestAnnotation extAnnot1 = (ExtensionTestAnnotation) annot1;
		Assert.assertEquals(strValue, extAnnot1.getNewStringProp1());
		Assert.assertEquals(intValue, extAnnot1.getNewIntProp2());

		/* This does not make sense as you would never enter ExtensionTestAnnotation.class as deserialization target
		 * which would enforce usage of the standard Bean serializer (since no serializer is registered for this specific class -> jsonProperties can not be mapped
		logger.info("Calling deserialization with " + ExtensionTestAnnotation.class.getSimpleName() + " as target");
		ExtensionTestAnnotation annot2 = om.readValue(newTestAnnotJSON, ExtensionTestAnnotation.class);
		Assert.assertNotNull(annot2);
		logger.info("Deserialized annotation JSON (target: " + ExtensionTestAnnotation.class.getSimpleName() + "): " + om.writeValueAsString(annot2));
		logger.info("Deserialized annotation class (target: " + ExtensionTestAnnotation.class.getSimpleName() + "): " + annot2.getClass().getName());
		Assert.assertEquals(ExtensionTestAnnotation.class, annot2.getClass());
		String s = annot2.getNewStringProp1();
		Assert.assertEquals(strValue, annot2.getNewStringProp1());
		Assert.assertEquals(intValue, annot2.getNewIntProp2()); */

		logger.info("Processing profiling annotation...");
		Annotation unknownAnnot = readJSONObjectFromFileInClasspath(om, Annotation.class, "org/apache/atlas/odf/core/test/annotation/annotexttest1.json", null);
		Assert.assertNotNull(unknownAnnot);
		logger.info("Read Unknown annotation: " + unknownAnnot.getClass().getName());
		Assert.assertEquals(ProfilingAnnotation.class, unknownAnnot.getClass());

		logger.info("Read profiling annotation: " + om.writeValueAsString(unknownAnnot));
		JSONObject jsonPropertiesObj = new JSONObject(unknownAnnot.getJsonProperties());
		Assert.assertEquals("newProp1Value", jsonPropertiesObj.get("newProp1"));
		Assert.assertEquals((Integer) 4237, jsonPropertiesObj.get("newProp2"));
	}
}
