/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.metadata;

import com.google.common.collect.ImmutableList;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;

import org.apache.hadoop.metadata.json.Serialization$;
import org.apache.hadoop.metadata.json.TypesSerialization;
import org.apache.hadoop.metadata.types.AttributeDefinition;
import org.apache.hadoop.metadata.types.ClassType;
import org.apache.hadoop.metadata.types.DataTypes;
import org.apache.hadoop.metadata.types.HierarchicalTypeDefinition;
import org.apache.hadoop.metadata.types.IDataType;
import org.apache.hadoop.metadata.types.Multiplicity;
import org.apache.hadoop.metadata.types.StructTypeDefinition;
import org.apache.hadoop.metadata.types.TraitType;
import org.apache.hadoop.metadata.types.TypeSystem;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import java.util.ArrayList;
import java.util.Arrays;

public class DemoDataDriver {

	private static ArrayList<ITypedReferenceableInstance> tableArray;
	private static ArrayList<ITypedReferenceableInstance> lineageArray;

	private static final Logger LOG = LoggerFactory
			.getLogger(DemoDataDriver.class);

	private static final String DATABASE_TYPE = "hive_database";
	private static final String TABLE_TYPE = "hive_table";

	protected TypeSystem typeSystem;
	protected WebResource service;

	public void setUp() throws Exception {
		typeSystem = TypeSystem.getInstance();
		typeSystem.reset();

		String baseUrl = "http://localhost:21000/";

		DefaultClientConfig config = new DefaultClientConfig();
		Client client = Client.create(config);
		client.resource(UriBuilder.fromUri(baseUrl).build());

		service = client.resource(UriBuilder.fromUri(baseUrl).build());
	}

	protected AttributeDefinition createRequiredAttrDef(String name,
			IDataType dataType) {
		return new AttributeDefinition(name, dataType.getName(),
				Multiplicity.REQUIRED, false, null);
	}

	@SuppressWarnings("unchecked")
	protected HierarchicalTypeDefinition<TraitType> createTraitTypeDef(
			String name, ImmutableList<String> superTypes,
			AttributeDefinition... attrDefs) {
		return new HierarchicalTypeDefinition(TraitType.class, name,
				superTypes, attrDefs);
	}

	@SuppressWarnings("unchecked")
	protected HierarchicalTypeDefinition<ClassType> createClassTypeDef(
			String name, ImmutableList<String> superTypes,
			AttributeDefinition... attrDefs) {
		return new HierarchicalTypeDefinition(ClassType.class, name,
				superTypes, attrDefs);
	}

	public void submitEntity(ITypedReferenceableInstance tableInstance)
			throws Exception {
		String tableInstanceAsJSON = Serialization$.MODULE$
				.toJson(tableInstance);
		LOG.debug("tableInstance = " + tableInstanceAsJSON);

		WebResource resource = service.path("api/metadata/entities/submit")
				.path(TABLE_TYPE);

		ClientResponse clientResponse = resource
				.accept(MediaType.APPLICATION_JSON)
				.type(MediaType.APPLICATION_JSON)
				.method(HttpMethod.POST, ClientResponse.class,
						tableInstanceAsJSON);
		assert clientResponse.getStatus() == Response.Status.OK.getStatusCode();
	}

	public void getEntityList() throws Exception {
		ClientResponse clientResponse = service
				.path("api/metadata/entities/list/").path(TABLE_TYPE)
				.accept(MediaType.APPLICATION_JSON)
				.type(MediaType.APPLICATION_JSON)
				.method(HttpMethod.GET, ClientResponse.class);
		assert clientResponse.getStatus() == Response.Status.OK.getStatusCode();

		String responseAsString = clientResponse.getEntity(String.class);
		JSONObject response = new JSONObject(responseAsString);
		final JSONArray list = response.getJSONArray("list");
		System.out.println("list = " + list);
		assert list != null;
		assert list.length() > 0;
	}

	private void createTypes() throws Exception {
		HierarchicalTypeDefinition<ClassType> databaseTypeDefinition = createClassTypeDef(
				DATABASE_TYPE, ImmutableList.<String> of(),
				createRequiredAttrDef("name", DataTypes.STRING_TYPE),
				createRequiredAttrDef("description", DataTypes.STRING_TYPE));

		StructTypeDefinition structTypeDefinition = new StructTypeDefinition(
				"serdeType", new AttributeDefinition[] {
						createRequiredAttrDef("name", DataTypes.STRING_TYPE),
						createRequiredAttrDef("serde", DataTypes.STRING_TYPE) });

		HierarchicalTypeDefinition<ClassType> tableTypeDefinition = createClassTypeDef(
				TABLE_TYPE, ImmutableList.<String> of(),
				createRequiredAttrDef("name", DataTypes.STRING_TYPE),
				createRequiredAttrDef("description", DataTypes.STRING_TYPE),
				createRequiredAttrDef("type", DataTypes.STRING_TYPE),
				new AttributeDefinition("serde1", "serdeType",
						Multiplicity.REQUIRED, false, null),
				new AttributeDefinition("serde2", "serdeType",
						Multiplicity.REQUIRED, false, null),
				new AttributeDefinition("database", DATABASE_TYPE,
						Multiplicity.REQUIRED, true, null));

		HierarchicalTypeDefinition<TraitType> classificationTypeDefinition = createTraitTypeDef(
				"classification", ImmutableList.<String> of(),
				createRequiredAttrDef("tag", DataTypes.STRING_TYPE));

		HierarchicalTypeDefinition<ClassType> lineageTypeDefinition = createClassTypeDef(
				"HiveLineage",
				ImmutableList.<String> of(),
				createRequiredAttrDef("queryId", DataTypes.STRING_TYPE),
				createRequiredAttrDef("hiveId", DataTypes.STRING_TYPE),
				createRequiredAttrDef("user", DataTypes.STRING_TYPE),
				createRequiredAttrDef("queryStartTime", DataTypes.STRING_TYPE),
				createRequiredAttrDef("queryEndTime", DataTypes.STRING_TYPE),
				createRequiredAttrDef("query", DataTypes.STRING_TYPE),
				new AttributeDefinition("tableName", TABLE_TYPE,
						Multiplicity.REQUIRED, true, null),
				createRequiredAttrDef("success", DataTypes.STRING_TYPE),
				createRequiredAttrDef("executionEngine", DataTypes.STRING_TYPE),
				new AttributeDefinition("sourceTables", DataTypes.arrayTypeName(TABLE_TYPE),
						Multiplicity.COLLECTION, true, "forwardLineage"));

		typeSystem.defineTypes(ImmutableList.of(structTypeDefinition),
				ImmutableList.of(classificationTypeDefinition), ImmutableList
						.of(databaseTypeDefinition, tableTypeDefinition,
								lineageTypeDefinition));
	}

	private void submitTypes() throws Exception {
		String tableTypesAsJSON = TypesSerialization.toJson(
				typeSystem,
				Arrays.asList(new String[] { DATABASE_TYPE, TABLE_TYPE,
						"serdeType", "classification"}));
		String lineageTypesAsJSON = TypesSerialization.toJson(
				typeSystem,
				Arrays.asList(new String[] { "HiveLineage" }));
		sumbitType(tableTypesAsJSON, TABLE_TYPE);
		sumbitType(lineageTypesAsJSON, "HiveLineage");
	}

	private void sumbitType(String typesAsJSON, String type)
			throws JSONException {
		WebResource resource = service.path("api/metadata/types/submit").path(
				type);

		ClientResponse clientResponse = resource
				.accept(MediaType.APPLICATION_JSON)
				.type(MediaType.APPLICATION_JSON)
				.method(HttpMethod.POST, ClientResponse.class, typesAsJSON);
		assert clientResponse.getStatus() == Response.Status.OK.getStatusCode();

		String responseAsString = clientResponse.getEntity(String.class);
		JSONObject response = new JSONObject(responseAsString);
		assert response.get("typeName").equals(type);
		assert response.get("types") != null;
	}

	private ITypedReferenceableInstance createHiveTableInstance(String db,
			String table, String trait, String serde1, String serde2)
			throws Exception {
		Referenceable databaseInstance = new Referenceable(DATABASE_TYPE);
		databaseInstance.set("name", db);
		databaseInstance.set("description", db + " database");

		Referenceable tableInstance = new Referenceable(TABLE_TYPE,
				"classification");
		tableInstance.set("name", table);
		tableInstance.set("description", table + " table");
		tableInstance.set("type", "managed");
		tableInstance.set("database", databaseInstance);

		Struct traitInstance = (Struct) tableInstance
				.getTrait("classification");
		traitInstance.set("tag", trait);

		Struct serde1Instance = new Struct("serdeType");
		serde1Instance.set("name", serde1);
		serde1Instance.set("serde", serde1);
		tableInstance.set("serde1", serde1Instance);

		Struct serde2Instance = new Struct("serdeType");
		serde2Instance.set("name", serde2);
		serde2Instance.set("serde", serde2);
		tableInstance.set("serde2", serde2Instance);

		ClassType tableType = typeSystem.getDataType(ClassType.class,
				TABLE_TYPE);
		return tableType.convert(tableInstance, Multiplicity.REQUIRED);
	}

	private ITypedReferenceableInstance createLingeageInstance(String queryId,
			String hiveId, String user, String queryStartTime,
			String queryEndTime, String query, String tableName,
			String success, String executionEngine, String sourceTables)
			throws Exception {
		Referenceable lineageInstance = new Referenceable("HiveLineage");
		lineageInstance.set("queryId", queryId);
		lineageInstance.set("hiveId", hiveId);
		lineageInstance.set("user", user);
		lineageInstance.set("queryStartTime", queryStartTime);
		lineageInstance.set("queryEndTime", queryEndTime);
		lineageInstance.set("query", query);
		lineageInstance.set("success", success);
		lineageInstance.set("executionEngine", executionEngine);

		for (ITypedReferenceableInstance table : tableArray) {
			if (table.get("name").equals(tableName)) {
				lineageInstance.set("tableName", table);
				break;
			}
		}
		ArrayList<ITypedReferenceableInstance> sourceTablesRefArr = new ArrayList<ITypedReferenceableInstance>();

		for (String s : sourceTables.split(",")) {
			System.out.println("search for table "+s);
			for (ITypedReferenceableInstance table : tableArray) {
				if (table.get("name").equals(s)) {
					sourceTablesRefArr.add(table);
				}
			}
		}

		lineageInstance.set("sourceTables",
				ImmutableList.copyOf(sourceTablesRefArr));

		ClassType lineageType = typeSystem.getDataType(ClassType.class,
				"HiveLineage");
		return lineageType.convert(lineageInstance, Multiplicity.REQUIRED);

	}

	public static void main(String[] args) throws Exception {
		DemoDataDriver driver = new DemoDataDriver();
		driver.setUp();

		driver.createTypes();
		driver.submitTypes();

		DemoDataDriver.tableArray = new ArrayList<ITypedReferenceableInstance>();
		DemoDataDriver.lineageArray = new ArrayList<ITypedReferenceableInstance>();

		String[][] tableData = getTestTableData();
		for (String[] row : tableData) {
			ITypedReferenceableInstance tableInstance = driver
					.createHiveTableInstance(row[0], row[1], row[2], row[3],
							row[4]);
			tableArray.add(tableInstance);
		}

		String[][] lineageData = getTestLineageData();
		for (String[] row : lineageData) {
			ITypedReferenceableInstance lineageInstance = driver
					.createLingeageInstance(row[0], row[1], row[2], row[3],
							row[4], row[5], row[6], row[7], row[8], row[9]);
			lineageArray.add(lineageInstance);
		}

		/*for (ITypedReferenceableInstance i : tableArray) {
			driver.submitEntity(i);
		}*/
		for (ITypedReferenceableInstance i : lineageArray) {
			driver.submitEntity(i);
		}

		driver.getEntityList();
	}

	private static String[][] getTestLineageData() {
		return new String[][] {
				{
						"s123456_20150106120303_036186d5-a991-4dfc-9ff2-05b072c7e711",
						"90797386-3933-4ab0-ae68-a7baa7e155d4",
						"Service User 02",
						"1420563838114",
						"1420563853806",
						"CREATE TABLE providerCharges AS SELECT providerMasterList.*, claimPayments.* FROM  providerMasterList LEFT JOIN claimPayments ON providerMasterList.providerID = claimPayments.providerId  WHERE claimPayments.paidStatus = \"true\";",
						"providerCharges", "true", "tez",
						"providerMasterList,claimPayments" },
				{
						"s123456_20150106120304_036125d5-a991-4dfc-9ff2-05b665c7e711",
						"90797386-3933-4ab0-ae68-a7baa72435d4",
						"Service User 02",
						"1420563838314",
						"1420563853906",
						"CREATE TABLE providerComparativeModel AS SELECT providerCharges.*, LocationsOfThings.* FROM  providerCharges LEFT JOIN LocationsOfThings ON providerCharges.providerName = LocationsOfThings.peopleName  WHERE LocationsOfThings.isDr = \"true\";",
						"providerComparativeModel", "true", "mapred",
						"providerCharges,LocationsOfThings" } };
	}

	private static String[][] getTestTableData() {
		return new String[][] {
				{
						"provider_db",
						"providerMasterList",
						"Providers Addresses and Locations of performed procedures",
						"org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
						"org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe" },
				{ "charges_db", "claimPayments", "Claims paid",
						"org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
						"org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe" },
				{ "model_db", "providerCharges",
						"Combined Claims and Providers Mapping",
						"org.apache.hadoop.hive.ql.io.orc.OrcSerde",
						"org.apache.hadoop.hive.ql.io.orc.OrcSerde" },
				{ "ds_db", "LocationsOfThings",
						"DS imported dataset from internet of ideas",
						"org.apache.hadoop.hive.ql.io.orc.OrcSerde",
						"org.apache.hadoop.hive.ql.io.orc.OrcSerde" },
				{
						"ds_db",
						"providerComparativeModel",
						"DS created Table for comparing charges findings to dataset from internet",
						"org.apache.hadoop.hive.ql.io.orc.OrcSerde",
						"org.apache.hadoop.hive.ql.io.orc.OrcSerde" }

		};
	}
}
