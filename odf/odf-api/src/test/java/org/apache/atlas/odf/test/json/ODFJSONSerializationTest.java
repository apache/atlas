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
package org.apache.atlas.odf.test.json;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.metadata.InvalidReference;
import org.apache.atlas.odf.api.metadata.StoredMetaDataObject;
import org.apache.wink.json4j.JSON;
import org.apache.wink.json4j.JSONException;
import org.apache.wink.json4j.JSONObject;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceEndpoint;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceProperties;
import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.metadata.models.Annotation;
import org.apache.atlas.odf.api.metadata.models.ClassificationAnnotation;
import org.apache.atlas.odf.api.metadata.models.JDBCConnection;
import org.apache.atlas.odf.api.metadata.models.JDBCConnectionInfo;
import org.apache.atlas.odf.api.metadata.models.MetaDataCache;
import org.apache.atlas.odf.api.metadata.models.MetaDataObject;
import org.apache.atlas.odf.api.metadata.models.Column;
import org.apache.atlas.odf.api.metadata.models.Connection;
import org.apache.atlas.odf.api.metadata.models.ConnectionInfo;
import org.apache.atlas.odf.api.metadata.models.DataFile;
import org.apache.atlas.odf.api.metadata.models.DataSet;
import org.apache.atlas.odf.api.metadata.models.DataStore;
import org.apache.atlas.odf.api.metadata.models.Database;
import org.apache.atlas.odf.api.metadata.models.Table;
import org.apache.atlas.odf.api.metadata.models.ProfilingAnnotation;
import org.apache.atlas.odf.api.metadata.models.RelationshipAnnotation;
import org.apache.atlas.odf.api.metadata.models.UnknownDataSet;
import org.apache.atlas.odf.json.JSONUtils;

public class ODFJSONSerializationTest {

	Logger logger = Logger.getLogger(ODFJSONSerializationTest.class.getName());

	MetaDataObjectReference createNewRef() {
		MetaDataObjectReference ref = new MetaDataObjectReference();
		ref.setId(UUID.randomUUID().toString());
		ref.setRepositoryId("odftestrepositoryid");
		return ref;
	}

	static class NewAnnotation extends ProfilingAnnotation {
		String newProp;

		public String getNewProp() {
			return newProp;
		}

		public void setNewProp(String newProp) {
			this.newProp = newProp;
		}

	}

	List<MetaDataObject> createTestObjects() throws JSONException, ParseException {
		List<MetaDataObject> testObjects = new ArrayList<>();

		Column col = new Column();
		MetaDataObjectReference colref = createNewRef();
		col.setReference(colref);
		col.setName("col1");
		col.setDescription("column desc");
		col.setDataType("theDatatype");

		Table t = new Table();
		MetaDataObjectReference tableRef = createNewRef();
		t.setReference(tableRef);
		t.setName("Table");
		t.setDescription("table desc");

		Database db = new Database();
		MetaDataObjectReference dbref = createNewRef();
		db.setReference(dbref);
		db.setName("DB");
		db.setDescription("db description");

		JDBCConnection jdbcConn = new JDBCConnection();
		MetaDataObjectReference jdbcConnRef = createNewRef();
		jdbcConn.setReference(jdbcConnRef);
		jdbcConn.setName("jdbc connection");
		jdbcConn.setUser("theUser");
		jdbcConn.setPassword("thePassword");
		jdbcConn.setJdbcConnectionString("jdbc:db2:localhost:50000/SAMPLE");
		db.setConnections(Collections.singletonList(jdbcConnRef));

		ProfilingAnnotation profAnnot1 = new ProfilingAnnotation();
		MetaDataObjectReference uaRef = createNewRef();
		profAnnot1.setReference(uaRef);
		profAnnot1.setProfiledObject(jdbcConnRef);
		profAnnot1.setJsonProperties("{\"a\": \"b\"}");

		ProfilingAnnotation profAnnot2 = new ProfilingAnnotation();
		MetaDataObjectReference mdoRef = createNewRef();
		profAnnot2.setReference(mdoRef);
		profAnnot2.setProfiledObject(jdbcConnRef);
		profAnnot2.setJsonProperties("{\"a\": \"b\"}");

		NewAnnotation newAnnot = new NewAnnotation();
		MetaDataObjectReference newAnnotRef = createNewRef();
		newAnnot.setReference(newAnnotRef);

		// a generic DataSet
		UnknownDataSet ds = new UnknownDataSet();
		ds.setName("generic data set");
		ds.setReference(createNewRef());

		MetaDataObject[] mdos = new MetaDataObject[] { db, jdbcConn, t, col, profAnnot1, profAnnot2, newAnnot, ds };
		testObjects.addAll(Arrays.asList(mdos));
		return testObjects;
	}

	@Test
	public void testSerialization() throws Exception {
		List<MetaDataObject> testObjects = createTestObjects();

		for (MetaDataObject testObject : testObjects) {
			Class<?> cl = testObject.getClass();
			logger.info("Testing serialization / deserialization of object: " + testObject + " of class: " + cl);

			String json = JSONUtils.toJSON(testObject);
			logger.info("Serialized json: " + json);

			Object objStronglyTypedClass;
			if (testObject instanceof Annotation) { // special treatment for Annotations -> 2nd arg of fromJSON() needs to be Annotation.class
				objStronglyTypedClass = JSONUtils.fromJSON(json, Annotation.class);
				Assert.assertEquals(cl, objStronglyTypedClass.getClass());
			}
			else {
				 objStronglyTypedClass = JSONUtils.fromJSON(json, cl);
				 Assert.assertEquals(cl, objStronglyTypedClass.getClass());
			}
			String json1 = JSONUtils.toJSON(objStronglyTypedClass);
			Assert.assertEquals(json, json1);

			Object objWithGenericClass = JSONUtils.fromJSON(json, MetaDataObject.class);

			Assert.assertEquals(cl, objWithGenericClass.getClass());
			String json2 = JSONUtils.toJSON(objWithGenericClass);
			Assert.assertEquals(json, json2);

			Class<?> intermediateClasses[] = new Class<?>[] { MetaDataObject.class, DataSet.class, DataStore.class, Connection.class };

			for (Class<?> intermediateClass : intermediateClasses) {
				logger.info("Checking intermediate class: " + intermediateClass);
				if (intermediateClass.isAssignableFrom(cl)) {

					Object intermediateObject = JSONUtils.fromJSON(json, intermediateClass);
					logger.info("Deserialized object: " + intermediateObject);
					logger.info("Deserialized object class: " + intermediateObject.getClass());

					Assert.assertTrue(intermediateClass.isAssignableFrom(intermediateObject.getClass()));
					Assert.assertEquals(cl, intermediateObject.getClass());
					String json3 = JSONUtils.toJSON(intermediateObject);
					Assert.assertEquals(json, json3);
				}
			}

		}
	}

	/**
	 * Test serialization of an Annotation (subclass) which has both, its own fields (to be mapped to jsonProperties) and
	 * a non-empty jsonProperties attribute holding the string representation of a Json object.
	 */

	@Test
	public void testJsonPropertiesMerge() {
		NewAnnotation annot = new NewAnnotation();
		MetaDataObjectReference ref = new MetaDataObjectReference();
		ref.setId("id");
		ref.setRepositoryId("repoid");
		ref.setUrl("http://url");
		annot.setProfiledObject(ref);
		annot.setNewProp("newPropValue");
		annot.setJsonProperties("{\"oldProp\":\"oldPropValue\"}");
		JSONObject jo = null;
		try {
			jo = JSONUtils.toJSONObject(annot);
			String jsonPropertiesString = jo.getString("jsonProperties");
			JSONObject jo2 = new JSONObject(jsonPropertiesString);
			Assert.assertEquals("oldPropValue", jo2.get("oldProp"));
			Assert.assertEquals("newPropValue", jo2.get("newProp"));
		}
		catch (JSONException e) {
			e.printStackTrace();
		}
	}

	final static private String MERGED_JSON = "{" +
			"\"analysisRun\":null," +
			"\"summary\":null," +
			"\"reference\":null," +
			"\"originRef\":null," +
			"\"replicaRefs\":null," +
			"\"javaClass\":\"org.apache.atlas.odf.json.test.ODFJSONSerializationTest$NewAnnotation\"," +
			"\"jsonProperties\":\"{" +
			   "\\\"newProp\\\":\\\"newPropValue\\\"," +
			   "\\\"oldProp\\\":\\\"oldPropValue\\\"" +
			   "}\"," +
			"\"name\":null," +
			"\"annotationType\":\"NewAnnotation\"," +
			"\"description\":null," +
			"\"profiledObject\":{" +
			   "\"repositoryId\":\"repoid\"," +
			   "\"id\":\"id\"," +
			   "\"url\":\"http://url\"}" +
	        "}";

	/**
	 * Test deserialization of a Json object which has fields in its jsonProperties that can not be mapped to native fields of
	 * the target class (= value of javaClass field). These and only these remain as fields in the text encoded Json object
	 * stored in the jsonProperties field of the result.
	 */

	@Test
	@Ignore
	public void testJsonPropertiesUnmerge() throws Exception {
		logger.info("Deserializing JSON: " + MERGED_JSON);
		Annotation annot = JSONUtils.fromJSON(MERGED_JSON, Annotation.class);
		Assert.assertTrue(annot instanceof NewAnnotation);
		NewAnnotation newAnnot = (NewAnnotation) annot;
		Assert.assertEquals("newPropValue", newAnnot.getNewProp());
		JSONObject props = (JSONObject) JSON.parse(annot.getJsonProperties());

		Assert.assertNotNull(props.get("oldProp"));
		Assert.assertEquals("oldPropValue", props.get("oldProp"));

		JSONObject jo = JSONUtils.toJSONObject(annot);
		Assert.assertEquals(MERGED_JSON, jo.toString());
	}

	final private static String PROFILING_ANNOTATION_JSON = "{" +
			"\"profiledObject\": null," +
			"\"annotationType\": \"MySubType1\"," +
			"\"javaClass\": \"org.apache.atlas.odf.core.integrationtest.metadata.atlas.MySubType1\"," +
			"\"analysisRun\": \"bla\"," +
			"\"newProp1\": 42," +
			"\"newProp2\": \"hi\"," +
			"\"newProp3\": \"hello\"" +
		"}";

	final private static String CLASSIFICATION_ANNOTATION_JSON = "{" +
			"\"classifyingObject\": null," +
			"\"classifiedObject\": null," +
			"\"annotationType\": \"MySubType2\"," +
			"\"javaClass\": \"org.apache.atlas.odf.core.integrationtest.metadata.atlas.MySubType2\"," +
			"\"analysisRun\": \"bla\"," +
			"\"newProp1\": 42," +
			"\"newProp2\": \"hi\"," +
			"\"newProp3\": \"hello\"" +
		"}";

	final private static String RELATIONSHIP_ANNOTATION_JSON = "{" +
			"\"relatedObjects\": null," +
			"\"annotationType\": \"MySubType3\"," +
			"\"javaClass\": \"org.apache.atlas.odf.core.integrationtest.metadata.atlas.MySubType3\"," +
			"\"analysisRun\": \"bla\"," +
			"\"newProp1\": 42," +
			"\"newProp2\": \"hi\"," +
			"\"newProp3\": \"hello\"" +
		"}";

	 /**
	  *  Replacement for AtlasAnnotationTypeDefinitionCreatTest
	  */

	@Test
	public void testSimpleAnnotationPrototypeCreation() throws Exception {
		logger.info("Annotation string: " + PROFILING_ANNOTATION_JSON);
		Annotation annot = JSONUtils.fromJSON(PROFILING_ANNOTATION_JSON, Annotation.class);
		logger.info("Annotation: " + PROFILING_ANNOTATION_JSON);
		Assert.assertTrue(annot instanceof ProfilingAnnotation);

		logger.info("Annotation string: " + CLASSIFICATION_ANNOTATION_JSON);
		annot = JSONUtils.fromJSON(CLASSIFICATION_ANNOTATION_JSON, Annotation.class);
		logger.info("Annotation: " + CLASSIFICATION_ANNOTATION_JSON);
		Assert.assertTrue(annot instanceof ClassificationAnnotation);

		logger.info("Annotation string: " + RELATIONSHIP_ANNOTATION_JSON);
		annot = JSONUtils.fromJSON(RELATIONSHIP_ANNOTATION_JSON, Annotation.class);
		logger.info("Annotation: " + RELATIONSHIP_ANNOTATION_JSON);
		Assert.assertTrue(annot instanceof RelationshipAnnotation);
	}

	@Test
	public void testUnretrievedReference() throws Exception {
		String repoId = "SomeRepoId";
		Column col = new Column();
		col.setName("name");
		col.setReference(InvalidReference.createInvalidReference(repoId));

		String json = JSONUtils.toJSON(col);
		Column col2 = JSONUtils.fromJSON(json, Column.class);
		Assert.assertTrue(InvalidReference.isInvalidRef(col2.getReference()));

		Database db = new Database();
		db.setName("database");

		JSONUtils.toJSON(db);

		db.setConnections(InvalidReference.createInvalidReferenceList(repoId));

		Database db2 = JSONUtils.fromJSON(JSONUtils.toJSON(db), Database.class);
		Assert.assertTrue(InvalidReference.isInvalidRefList(db2.getConnections()));
	}

	@Test
	public void testExtensibleDiscoveryServiceEndpoints() throws Exception {
		DiscoveryServiceProperties dsprops = new DiscoveryServiceProperties();
		dsprops.setId("theid");
		dsprops.setName("thename");

		DiscoveryServiceEndpoint ep = new DiscoveryServiceEndpoint();
		ep.setRuntimeName("newruntime");
		ep.set("someKey", "someValue");
		dsprops.setEndpoint(ep);

		String dspropsJSON = JSONUtils.toJSON(dsprops);
		logger.info("Discovery service props JSON: " +dspropsJSON);

		DiscoveryServiceProperties deserProps = JSONUtils.fromJSON(dspropsJSON, DiscoveryServiceProperties.class);
		Assert.assertNotNull(deserProps);
		Assert.assertEquals("theid", dsprops.getId());
		Assert.assertEquals("thename", dsprops.getName());
		Assert.assertNotNull(deserProps.getEndpoint());
		Assert.assertTrue(deserProps.getEndpoint() instanceof DiscoveryServiceEndpoint);
		Assert.assertTrue(deserProps.getEndpoint().getClass().equals(DiscoveryServiceEndpoint.class));
		DiscoveryServiceEndpoint deserEP = (DiscoveryServiceEndpoint) deserProps.getEndpoint();
		Assert.assertEquals("newruntime", deserEP.getRuntimeName());
		Assert.assertEquals("someValue", deserEP.get().get("someKey"));
	}

	@Test
	public void testMetaDataCache() {
		MetaDataCache cache = new MetaDataCache();

		MetaDataObjectReference ref = new MetaDataObjectReference();
		ref.setId("id");
		ref.setRepositoryId("repositoryId");
		DataFile dataFile = new DataFile();
		dataFile.setName("dataFile");
		dataFile.setEncoding("encoding");
		dataFile.setReference(ref);

		List<MetaDataObjectReference> refList = new ArrayList<MetaDataObjectReference>();
		refList.add(ref);
		StoredMetaDataObject storedObject = new StoredMetaDataObject(dataFile);
		HashMap<String, List<MetaDataObjectReference>> referenceMap = new HashMap<String, List<MetaDataObjectReference>>();
		referenceMap.put("id", refList);
		storedObject.setReferencesMap(referenceMap);
		List<StoredMetaDataObject> metaDataObjects = new ArrayList<StoredMetaDataObject>();
		metaDataObjects.add(storedObject);
		cache.setMetaDataObjects(metaDataObjects);

		Connection con = new JDBCConnection();
		con.setName("connection");
		JDBCConnectionInfo conInfo = new JDBCConnectionInfo();
		conInfo.setConnections(Collections.singletonList(con));
		conInfo.setAssetReference(ref);
		conInfo.setTableName("tableName");
		List<ConnectionInfo> connectionInfoObjects = new ArrayList<ConnectionInfo>();
		connectionInfoObjects.add(conInfo);
		cache.setConnectionInfoObjects(connectionInfoObjects);

		try {
			String serializedCache = JSONUtils.toJSON(cache);
			logger.info("Serialized metadata cache JSON: " + serializedCache);
			MetaDataCache deserializedCache = JSONUtils.fromJSON(serializedCache, MetaDataCache.class);
			Assert.assertEquals("dataFile", deserializedCache.getMetaDataObjects().get(0).getMetaDataObject().getName());
			Assert.assertEquals("encoding", ((DataFile) deserializedCache.getMetaDataObjects().get(0).getMetaDataObject()).getEncoding());
			Assert.assertEquals("connection", deserializedCache.getConnectionInfoObjects().get(0).getConnections().get(0).getName());
			Assert.assertEquals("tableName", ((JDBCConnectionInfo) deserializedCache.getConnectionInfoObjects().get(0)).getTableName());
			Assert.assertEquals("repositoryId", deserializedCache.getMetaDataObjects().get(0).getReferenceMap().get("id").get(0).getRepositoryId());
		}
		catch (JSONException e) {
			e.printStackTrace();
		}
	}


}
