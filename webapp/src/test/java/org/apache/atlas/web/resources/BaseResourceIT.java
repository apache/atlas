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

package org.apache.atlas.web.resources;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import kafka.consumer.ConsumerTimeoutException;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.notification.NotificationConsumer;
import org.apache.atlas.notification.entity.EntityNotification;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.EnumValue;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeUtils;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.apache.atlas.utils.AuthenticationUtil;
import org.apache.atlas.utils.ParamChecker;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.RandomStringUtils;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.util.List;

/**
 * Base class for integration tests.
 * Sets up the web resource and has helper methods to create type and entity.
 */
public abstract class BaseResourceIT {

    public static final String ATLAS_REST_ADDRESS = "atlas.rest.address";
    protected WebResource service;
    protected AtlasClient serviceClient;
    public static final Logger LOG = LoggerFactory.getLogger(BaseResourceIT.class);
    protected static final int MAX_WAIT_TIME = 60000;
    protected String baseUrl;

    @BeforeClass
    public void setUp() throws Exception {

        DefaultClientConfig config = new DefaultClientConfig();
        Client client = Client.create(config);
        Configuration configuration = ApplicationProperties.get();
        baseUrl = configuration.getString(ATLAS_REST_ADDRESS, "http://localhost:21000/");
        client.resource(UriBuilder.fromUri(baseUrl).build());

        service = client.resource(UriBuilder.fromUri(baseUrl).build());

        if (!AuthenticationUtil.isKerberosAuthenticationEnabled()) {
            serviceClient = new AtlasClient(new String[]{baseUrl}, new String[]{"admin", "admin"});
        } else {
            serviceClient = new AtlasClient(baseUrl);
        }
    }

    protected void createType(TypesDef typesDef) throws Exception {
        HierarchicalTypeDefinition<ClassType> sampleType = typesDef.classTypesAsJavaList().get(0);
        try {
            serviceClient.getType(sampleType.typeName);
            LOG.info("Types already exist. Skipping type creation");
        } catch(AtlasServiceException ase) {
            //Expected if type doesnt exist
            String typesAsJSON = TypesSerialization.toJson(typesDef);
            createType(typesAsJSON);
        }
    }

    protected void createType(String typesAsJSON) throws Exception {
        WebResource resource = service.path("api/atlas/types");

        ClientResponse clientResponse = resource.accept(Servlets.JSON_MEDIA_TYPE).type(Servlets.JSON_MEDIA_TYPE)
                .method(HttpMethod.POST, ClientResponse.class, typesAsJSON);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.CREATED.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get("types"));
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));
    }

    protected Id createInstance(Referenceable referenceable) throws Exception {
        String typeName = referenceable.getTypeName();
        System.out.println("creating instance of type " + typeName);

        String entityJSON = InstanceSerialization.toJson(referenceable, true);
        System.out.println("Submitting new entity= " + entityJSON);
        List<String> guids = serviceClient.createEntity(entityJSON);
        System.out.println("created instance for type " + typeName + ", guid: " + guids);

        // return the reference to created instance with guid
        if (guids.size() > 0) {
            return new Id(guids.get(guids.size() - 1), 0, referenceable.getTypeName());
        }
        return null;
    }

    protected static final String DATABASE_TYPE = "hive_db";
    protected static final String HIVE_TABLE_TYPE = "hive_table";
    protected static final String COLUMN_TYPE = "hive_column";
    protected static final String HIVE_PROCESS_TYPE = "hive_process";

    protected void createTypeDefinitions() throws Exception {
        HierarchicalTypeDefinition<ClassType> dbClsDef = TypesUtil
                .createClassTypeDef(DATABASE_TYPE, null,
                        TypesUtil.createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        TypesUtil.createRequiredAttrDef("description", DataTypes.STRING_TYPE),
                        attrDef("locationUri", DataTypes.STRING_TYPE),
                        attrDef("owner", DataTypes.STRING_TYPE), attrDef("createTime", DataTypes.INT_TYPE));

        HierarchicalTypeDefinition<ClassType> columnClsDef = TypesUtil
                .createClassTypeDef(COLUMN_TYPE, null, attrDef("name", DataTypes.STRING_TYPE),
                        attrDef("dataType", DataTypes.STRING_TYPE), attrDef("comment", DataTypes.STRING_TYPE));

        StructTypeDefinition structTypeDefinition = new StructTypeDefinition("serdeType",
                new AttributeDefinition[]{TypesUtil.createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        TypesUtil.createRequiredAttrDef("serde", DataTypes.STRING_TYPE)});

        EnumValue values[] = {new EnumValue("MANAGED", 1), new EnumValue("EXTERNAL", 2),};

        EnumTypeDefinition enumTypeDefinition = new EnumTypeDefinition("tableType", values);

        HierarchicalTypeDefinition<ClassType> tblClsDef = TypesUtil
                .createClassTypeDef(HIVE_TABLE_TYPE, ImmutableSet.of("DataSet"),
                        attrDef("owner", DataTypes.STRING_TYPE), attrDef("createTime", DataTypes.LONG_TYPE),
                        attrDef("lastAccessTime", DataTypes.DATE_TYPE),
                        attrDef("temporary", DataTypes.BOOLEAN_TYPE),
                        new AttributeDefinition("db", DATABASE_TYPE, Multiplicity.REQUIRED, true, null),
                        new AttributeDefinition("columns", DataTypes.arrayTypeName(COLUMN_TYPE),
                                Multiplicity.OPTIONAL, true, null),
                new AttributeDefinition("tableType", "tableType", Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("serde1", "serdeType", Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("serde2", "serdeType", Multiplicity.OPTIONAL, false, null));

        HierarchicalTypeDefinition<ClassType> loadProcessClsDef = TypesUtil
                .createClassTypeDef(HIVE_PROCESS_TYPE, ImmutableSet.of("Process"),
                        attrDef("userName", DataTypes.STRING_TYPE), attrDef("startTime", DataTypes.INT_TYPE),
                        attrDef("endTime", DataTypes.LONG_TYPE),
                        attrDef("queryText", DataTypes.STRING_TYPE, Multiplicity.REQUIRED),
                        attrDef("queryPlan", DataTypes.STRING_TYPE, Multiplicity.REQUIRED),
                        attrDef("queryId", DataTypes.STRING_TYPE, Multiplicity.REQUIRED),
                        attrDef("queryGraph", DataTypes.STRING_TYPE, Multiplicity.REQUIRED));

        HierarchicalTypeDefinition<TraitType> classificationTrait = TypesUtil
                .createTraitTypeDef("classification", ImmutableSet.<String>of(),
                        TypesUtil.createRequiredAttrDef("tag", DataTypes.STRING_TYPE));
        HierarchicalTypeDefinition<TraitType> piiTrait =
                TypesUtil.createTraitTypeDef("pii", ImmutableSet.<String>of());
        HierarchicalTypeDefinition<TraitType> phiTrait =
                TypesUtil.createTraitTypeDef("phi", ImmutableSet.<String>of());
        HierarchicalTypeDefinition<TraitType> pciTrait =
                TypesUtil.createTraitTypeDef("pci", ImmutableSet.<String>of());
        HierarchicalTypeDefinition<TraitType> soxTrait =
                TypesUtil.createTraitTypeDef("sox", ImmutableSet.<String>of());
        HierarchicalTypeDefinition<TraitType> secTrait =
                TypesUtil.createTraitTypeDef("sec", ImmutableSet.<String>of());
        HierarchicalTypeDefinition<TraitType> financeTrait =
                TypesUtil.createTraitTypeDef("finance", ImmutableSet.<String>of());

        TypesDef typesDef = TypesUtil.getTypesDef(ImmutableList.of(enumTypeDefinition),
                ImmutableList.of(structTypeDefinition),
                ImmutableList.of(classificationTrait, piiTrait, phiTrait, pciTrait, soxTrait, secTrait, financeTrait),
                ImmutableList.of(dbClsDef, columnClsDef, tblClsDef, loadProcessClsDef));

        createType(typesDef);
    }

    AttributeDefinition attrDef(String name, IDataType dT) {
        return attrDef(name, dT, Multiplicity.OPTIONAL, false, null);
    }

    AttributeDefinition attrDef(String name, IDataType dT, Multiplicity m) {
        return attrDef(name, dT, m, false, null);
    }

    AttributeDefinition attrDef(String name, IDataType dT, Multiplicity m, boolean isComposite,
                                String reverseAttributeName) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(dT);
        return new AttributeDefinition(name, dT.getName(), m, isComposite, reverseAttributeName);
    }

    protected String randomString() {
        return RandomStringUtils.randomAlphanumeric(10);
    }

    protected Referenceable createHiveTableInstance(String dbName, String tableName) throws Exception {
        Referenceable databaseInstance = new Referenceable(DATABASE_TYPE);
        databaseInstance.set("name", dbName);
        databaseInstance.set("description", "foo database");

        Referenceable tableInstance =
                new Referenceable(HIVE_TABLE_TYPE, "classification", "pii", "phi", "pci", "sox", "sec", "finance");
        tableInstance.set("name", tableName);
        tableInstance.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, tableName);
        tableInstance.set("db", databaseInstance);
        tableInstance.set("description", "bar table");
        tableInstance.set("lastAccessTime", "2014-07-11T08:00:00.000Z");
        tableInstance.set("type", "managed");
        tableInstance.set("level", 2);
        tableInstance.set("tableType", 1); // enum
        tableInstance.set("compressed", false);

        Struct traitInstance = (Struct) tableInstance.getTrait("classification");
        traitInstance.set("tag", "foundation_etl");

        Struct serde1Instance = new Struct("serdeType");
        serde1Instance.set("name", "serde1");
        serde1Instance.set("serde", "serde1");
        tableInstance.set("serde1", serde1Instance);

        Struct serde2Instance = new Struct("serdeType");
        serde2Instance.set("name", "serde2");
        serde2Instance.set("serde", "serde2");
        tableInstance.set("serde2", serde2Instance);

        List<String> traits = tableInstance.getTraits();
        Assert.assertEquals(traits.size(), 7);

        return tableInstance;
    }

    public interface Predicate {

        /**
         * Perform a predicate evaluation.
         *
         * @return the boolean result of the evaluation.
         * @throws Exception thrown if the predicate evaluation could not evaluate.
         */
        boolean evaluate() throws Exception;
    }

    public interface NotificationPredicate {

        /**
         * Perform a predicate evaluation.
         *
         * @return the boolean result of the evaluation.
         * @throws Exception thrown if the predicate evaluation could not evaluate.
         */
        boolean evaluate(EntityNotification notification) throws Exception;
    }

    /**
     * Wait for a condition, expressed via a {@link Predicate} to become true.
     *
     * @param timeout maximum time in milliseconds to wait for the predicate to become true.
     * @param predicate predicate waiting on.
     */
    protected void waitFor(int timeout, Predicate predicate) throws Exception {
        ParamChecker.notNull(predicate, "predicate");
        long mustEnd = System.currentTimeMillis() + timeout;

        boolean eval;
        while (!(eval = predicate.evaluate()) && System.currentTimeMillis() < mustEnd) {
            LOG.info("Waiting up to {} msec", mustEnd - System.currentTimeMillis());
            Thread.sleep(100);
        }
        if (!eval) {
            throw new Exception("Waiting timed out after " + timeout + " msec");
        }
    }

    protected EntityNotification waitForNotification(final NotificationConsumer<EntityNotification> consumer, int maxWait,
                                                     final NotificationPredicate predicate) throws Exception {
        final TypeUtils.Pair<EntityNotification, String> pair = TypeUtils.Pair.of(null, null);
        final long maxCurrentTime = System.currentTimeMillis() + maxWait;
        waitFor(maxWait, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                try {
                    while (consumer.hasNext() && System.currentTimeMillis() < maxCurrentTime) {
                        EntityNotification notification = consumer.next();
                        if (predicate.evaluate(notification)) {
                            pair.left = notification;
                            return true;
                        }
                    }
                } catch(ConsumerTimeoutException e) {
                    //ignore
                }
                return false;
            }
        });
        return pair.left;
    }

    protected NotificationPredicate newNotificationPredicate(final EntityNotification.OperationType operationType,
                                                             final String typeName, final String guid) {
        return new NotificationPredicate() {
            @Override
            public boolean evaluate(EntityNotification notification) throws Exception {
                return notification != null &&
                        notification.getOperationType() == operationType &&
                        notification.getEntity().getTypeName().equals(typeName) &&
                        notification.getEntity().getId()._getId().equals(guid);
            }
        };
    }
}
