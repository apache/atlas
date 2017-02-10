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

import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef.CONSTRAINT_TYPE_OWNED_REF;
import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef.CONSTRAINT_TYPE_INVERSE_REF;
import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef.CONSTRAINT_PARAM_ATTRIBUTE;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasDiscoveryClientV2;
import org.apache.atlas.AtlasEntitiesClientV2;
import org.apache.atlas.AtlasLineageClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.AtlasTypedefClientV2;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.Cardinality;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.notification.NotificationConsumer;
import org.apache.atlas.notification.entity.EntityNotification;
import org.apache.atlas.type.AtlasTypeUtil;
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
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.RandomStringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import kafka.consumer.ConsumerTimeoutException;

/**
 * Base class for integration tests.
 * Sets up the web resource and has helper methods to create type and entity.
 */
public abstract class BaseResourceIT {

    public static final String ATLAS_REST_ADDRESS = "atlas.rest.address";
    public static final String NAME = "name";
    public static final String QUALIFIED_NAME = "qualifiedName";
    public static final String CLUSTER_NAME = "clusterName";
    public static final String DESCRIPTION = "description";

    // All service clients
    protected AtlasClient atlasClientV1;
    protected AtlasTypedefClientV2 typedefClientV2;
    protected AtlasEntitiesClientV2 entitiesClientV2;
    protected AtlasDiscoveryClientV2 discoveryClientV2;
    protected AtlasLineageClientV2 lineageClientV2;

    public static final Logger LOG = LoggerFactory.getLogger(BaseResourceIT.class);
    protected static final int MAX_WAIT_TIME = 60000;
    protected String[] atlasUrls;

    @BeforeClass
    public void setUp() throws Exception {

        //set high timeouts so that tests do not fail due to read timeouts while you
        //are stepping through the code in a debugger
        ApplicationProperties.get().setProperty("atlas.client.readTimeoutMSecs", "100000000");
        ApplicationProperties.get().setProperty("atlas.client.connectTimeoutMSecs", "100000000");


        Configuration configuration = ApplicationProperties.get();
        atlasUrls = configuration.getStringArray(ATLAS_REST_ADDRESS);

        if (atlasUrls == null || atlasUrls.length == 0) {
            atlasUrls = new String[] { "http://localhost:21000/" };
        }

        if (!AuthenticationUtil.isKerberosAuthenticationEnabled()) {
            atlasClientV1 = new AtlasClient(atlasUrls, new String[]{"admin", "admin"});
            typedefClientV2 = new AtlasTypedefClientV2(atlasUrls, new String[]{"admin", "admin"});
            entitiesClientV2 = new AtlasEntitiesClientV2(atlasUrls, new String[]{"admin", "admin"});
            discoveryClientV2 = new AtlasDiscoveryClientV2(atlasUrls, new String[]{"admin", "admin"});
            lineageClientV2 = new AtlasLineageClientV2(atlasUrls, new String[]{"admin", "admin"});
        } else {
            atlasClientV1 = new AtlasClient(atlasUrls);
            typedefClientV2 = new AtlasTypedefClientV2(atlasUrls);
            entitiesClientV2 = new AtlasEntitiesClientV2(atlasUrls);
            discoveryClientV2 = new AtlasDiscoveryClientV2(atlasUrls);
            lineageClientV2 = new AtlasLineageClientV2(atlasUrls);
        }
    }

    protected void batchCreateTypes(AtlasTypesDef typesDef) throws AtlasServiceException { 
        for (AtlasEnumDef enumDef : typesDef.getEnumDefs()) {
            try {
                typedefClientV2.createEnumDef(enumDef);
            } catch (AtlasServiceException ex) {
                LOG.warn("EnumDef creation failed for {}", enumDef.getName());
            }
        }
        for (AtlasStructDef structDef : typesDef.getStructDefs()) {
            try {
                typedefClientV2.createStructDef(structDef);
            } catch (AtlasServiceException ex) {
                LOG.warn("StructDef creation failed for {}", structDef.getName());
            }
        }
        
            AtlasTypesDef entityDefs = new AtlasTypesDef(
            Collections.<AtlasEnumDef>emptyList(),
            Collections.<AtlasStructDef>emptyList(),
            Collections.<AtlasClassificationDef>emptyList(),
            typesDef.getEntityDefs());
        try {
            typedefClientV2.createAtlasTypeDefs(entityDefs);
        }
        catch(AtlasServiceException e) {
            LOG.warn("Type creation failed for {}", typesDef.toString());
            LOG.warn(e.toString());
        }
        
        for (AtlasClassificationDef classificationDef : typesDef.getClassificationDefs()) {
            try {
                typedefClientV2.createClassificationDef(classificationDef);
            } catch (AtlasServiceException ex) {
                LOG.warn("ClassificationDef creation failed for {}", classificationDef.getName());
            }
        }

    }
    
    protected void createType(AtlasTypesDef typesDef) {
        // Since the bulk create bails out on a single failure, this has to be done as a workaround
        for (AtlasEnumDef enumDef : typesDef.getEnumDefs()) {
            try {
                typedefClientV2.createEnumDef(enumDef);
            } catch (AtlasServiceException ex) {
                LOG.warn("EnumDef creation failed for {}", enumDef.getName());
            }
        }
        for (AtlasStructDef structDef : typesDef.getStructDefs()) {
            try {
                typedefClientV2.createStructDef(structDef);
            } catch (AtlasServiceException ex) {
                LOG.warn("StructDef creation failed for {}", structDef.getName());
            }
        }
        for (AtlasEntityDef entityDef : typesDef.getEntityDefs()) {
            try {
                typedefClientV2.createEntityDef(entityDef);
            } catch (AtlasServiceException ex) {
                LOG.warn("EntityDef creation failed for {}", entityDef.getName());
            }
        }
        for (AtlasClassificationDef classificationDef : typesDef.getClassificationDefs()) {
            try {
                typedefClientV2.createClassificationDef(classificationDef);
            } catch (AtlasServiceException ex) {
                LOG.warn("ClassificationDef creation failed for {}", classificationDef.getName());
            }
        }

    }

    protected void createType(TypesDef typesDef) throws Exception {
        try{
            if ( !typesDef.enumTypes().isEmpty() ){
                String sampleType = typesDef.enumTypesAsJavaList().get(0).name;
                atlasClientV1.getType(sampleType);
                LOG.info("Checking enum type existence");
            }
            else if( !typesDef.structTypes().isEmpty()){
                StructTypeDefinition sampleType = typesDef.structTypesAsJavaList().get(0);
                atlasClientV1.getType(sampleType.typeName);
                LOG.info("Checking struct type existence");
            }
            else if( !typesDef.traitTypes().isEmpty()){
                HierarchicalTypeDefinition<TraitType> sampleType = typesDef.traitTypesAsJavaList().get(0);
                atlasClientV1.getType(sampleType.typeName);
                LOG.info("Checking trait type existence");
            }
            else{
                HierarchicalTypeDefinition<ClassType> sampleType = typesDef.classTypesAsJavaList().get(0);
                atlasClientV1.getType(sampleType.typeName);
                LOG.info("Checking class type existence");
            }
            LOG.info("Types already exist. Skipping type creation");
        } catch(AtlasServiceException ase) {
            //Expected if type doesn't exist
            String typesAsJSON = TypesSerialization.toJson(typesDef);
            createType(typesAsJSON);
        }
    }

    protected List<String> createType(String typesAsJSON) throws Exception {
        return atlasClientV1.createType(TypesSerialization.fromJson(typesAsJSON));
    }

    protected Id createInstance(Referenceable referenceable) throws Exception {
        String typeName = referenceable.getTypeName();
        System.out.println("creating instance of type " + typeName);

        String entityJSON = InstanceSerialization.toJson(referenceable, true);
        System.out.println("Submitting new entity= " + entityJSON);
        List<String> guids = atlasClientV1.createEntity(entityJSON);
        System.out.println("created instance for type " + typeName + ", guid: " + guids);

        // return the reference to created instance with guid
        if (guids.size() > 0) {
            return new Id(guids.get(guids.size() - 1), 0, referenceable.getTypeName());
        }
        return null;
    }

    protected TypesDef getTypesDef(ImmutableList<EnumTypeDefinition> enums,
                                   ImmutableList<StructTypeDefinition> structs,
                                   ImmutableList<HierarchicalTypeDefinition<TraitType>> traits,
                                   ImmutableList<HierarchicalTypeDefinition<ClassType>> classes){
        enums = (enums != null) ? enums : ImmutableList
                .<EnumTypeDefinition>of();
        structs =
                (structs != null) ? structs : ImmutableList.<StructTypeDefinition>of();

        traits = (traits != null) ? traits : ImmutableList
                .<HierarchicalTypeDefinition<TraitType>>of();

        classes = (classes != null) ? classes : ImmutableList
                .<HierarchicalTypeDefinition<ClassType>>of();
        return TypesUtil.getTypesDef(enums, structs, traits, classes);

    }

    protected AtlasEntityHeader modifyEntity(AtlasEntity atlasEntity, boolean update) {
        EntityMutationResponse entity = null;
        try {
            if (!update) {
                entity = entitiesClientV2.createEntity(new AtlasEntityWithExtInfo(atlasEntity));
                assertNotNull(entity);
                assertNotNull(entity.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE));
                assertTrue(entity.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE).size() > 0);
                return entity.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE).get(0);
            } else {
                entity = entitiesClientV2.updateEntity(new AtlasEntityWithExtInfo(atlasEntity));
                assertNotNull(entity);
                assertNotNull(entity.getEntitiesByOperation(EntityMutations.EntityOperation.UPDATE));
                assertTrue(entity.getEntitiesByOperation(EntityMutations.EntityOperation.UPDATE).size() > 0);
                return entity.getEntitiesByOperation(EntityMutations.EntityOperation.UPDATE).get(0);
            }

        } catch (AtlasServiceException e) {
            LOG.error("Entity {} failed", update ? "update" : "creation", entity);
        }
        return null;
    }

    protected AtlasEntityHeader createEntity(AtlasEntity atlasEntity) {
        return modifyEntity(atlasEntity, false);
    }

    protected AtlasEntityHeader updateEntity(AtlasEntity atlasEntity) {
        return modifyEntity(atlasEntity, true);
    }

    protected static final String DATABASE_TYPE_V2 = "hive_db_v2";
    protected static final String HIVE_TABLE_TYPE_V2 = "hive_table_v2";
    protected static final String COLUMN_TYPE_V2 = "hive_column_v2";
    protected static final String HIVE_PROCESS_TYPE_V2 = "hive_process_v2";

    protected static final String DATABASE_TYPE = "hive_db_v1";
    protected static final String HIVE_TABLE_TYPE = "hive_table_v1";
    protected static final String COLUMN_TYPE = "hive_column_v1";
    protected static final String HIVE_PROCESS_TYPE = "hive_process_v1";

    protected static final String DATABASE_TYPE_BUILTIN = "hive_db";
    protected static final String HIVE_TABLE_TYPE_BUILTIN = "hive_table";
    protected static final String COLUMN_TYPE_BUILTIN = "hive_column";
    protected static final String HIVE_PROCESS_TYPE_BUILTIN = "hive_process";

    protected void createTypeDefinitionsV1() throws Exception {
        HierarchicalTypeDefinition<ClassType> dbClsDef = TypesUtil
                .createClassTypeDef(DATABASE_TYPE, null,
                        TypesUtil.createUniqueRequiredAttrDef(NAME, DataTypes.STRING_TYPE),
                        TypesUtil.createRequiredAttrDef(DESCRIPTION, DataTypes.STRING_TYPE),
                        attrDef("locationUri", DataTypes.STRING_TYPE),
                        attrDef("owner", DataTypes.STRING_TYPE), attrDef("createTime", DataTypes.INT_TYPE),
                        new AttributeDefinition("tables", DataTypes.arrayTypeName(HIVE_TABLE_TYPE),
                                Multiplicity.OPTIONAL, false, "db")
                        );

        HierarchicalTypeDefinition<ClassType> columnClsDef = TypesUtil
                .createClassTypeDef(COLUMN_TYPE, null, attrDef(NAME, DataTypes.STRING_TYPE),
                        attrDef("dataType", DataTypes.STRING_TYPE), attrDef("comment", DataTypes.STRING_TYPE));

        StructTypeDefinition structTypeDefinition = new StructTypeDefinition("serdeType",
                new AttributeDefinition[]{TypesUtil.createRequiredAttrDef(NAME, DataTypes.STRING_TYPE),
                        TypesUtil.createRequiredAttrDef("serde", DataTypes.STRING_TYPE)});

        EnumValue values[] = {new EnumValue("MANAGED", 1), new EnumValue("EXTERNAL", 2),};

        EnumTypeDefinition enumTypeDefinition = new EnumTypeDefinition("tableType", values);

        HierarchicalTypeDefinition<ClassType> tblClsDef = TypesUtil
                .createClassTypeDef(HIVE_TABLE_TYPE, ImmutableSet.of("DataSet"),
                        attrDef("owner", DataTypes.STRING_TYPE), attrDef("createTime", DataTypes.LONG_TYPE),
                        attrDef("lastAccessTime", DataTypes.DATE_TYPE),
                        attrDef("temporary", DataTypes.BOOLEAN_TYPE),
                        new AttributeDefinition("db", DATABASE_TYPE, Multiplicity.OPTIONAL, true, "tables"),
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
        /*HierarchicalTypeDefinition<TraitType> factTrait =
                TypesUtil.createTraitTypeDef("Fact", ImmutableSet.<String>of());
        HierarchicalTypeDefinition<TraitType> etlTrait =
                TypesUtil.createTraitTypeDef("ETL", ImmutableSet.<String>of());
        HierarchicalTypeDefinition<TraitType> dimensionTrait =
                TypesUtil.createTraitTypeDef("Dimension", ImmutableSet.<String>of());
        HierarchicalTypeDefinition<TraitType> metricTrait =
                TypesUtil.createTraitTypeDef("Metric", ImmutableSet.<String>of());*/

        createType(getTypesDef(ImmutableList.of(enumTypeDefinition), null, null, null));
        createType(getTypesDef(null, ImmutableList.of(structTypeDefinition), null, null));
        createType(getTypesDef(null, null,
                 ImmutableList.of(classificationTrait, piiTrait, phiTrait, pciTrait,
                         soxTrait, secTrait, financeTrait), null));
        createType(getTypesDef(null, null, null,
                ImmutableList.of(dbClsDef, columnClsDef, tblClsDef, loadProcessClsDef)));
    }

    protected void createTypeDefinitionsV2() throws Exception {
        
        AtlasConstraintDef isCompositeSourceConstraint = new AtlasConstraintDef(CONSTRAINT_TYPE_OWNED_REF);

        AtlasConstraintDef isCompositeTargetConstraint = new AtlasConstraintDef(CONSTRAINT_TYPE_INVERSE_REF,
                Collections.<String, Object>singletonMap(CONSTRAINT_PARAM_ATTRIBUTE, "randomTable"));

        AtlasEntityDef dbClsTypeDef = AtlasTypeUtil.createClassTypeDef(
                DATABASE_TYPE_V2,
                null,
                AtlasTypeUtil.createUniqueRequiredAttrDef(NAME, "string"),
                AtlasTypeUtil.createRequiredAttrDef(DESCRIPTION, "string"),
                AtlasTypeUtil.createOptionalAttrDef("locationUri", "string"),
                AtlasTypeUtil.createOptionalAttrDef("owner", "string"),
                AtlasTypeUtil.createOptionalAttrDef("createTime", "int"),
                AtlasTypeUtil.createOptionalAttrDef("createTime", "int"), 
                //there is a serializ
                new AtlasAttributeDef("randomTable", 
                        DataTypes.arrayTypeName(HIVE_TABLE_TYPE_V2),
                        true,
                        Cardinality.SET,
                        0, -1, false, true, Collections.singletonList(isCompositeSourceConstraint))
        );

        AtlasEntityDef columnClsDef = AtlasTypeUtil
                .createClassTypeDef(COLUMN_TYPE_V2, null,
                        AtlasTypeUtil.createOptionalAttrDef(NAME, "string"),
                        AtlasTypeUtil.createOptionalAttrDef("dataType", "string"),
                        AtlasTypeUtil.createOptionalAttrDef("comment", "string"));

        AtlasStructDef structTypeDef = AtlasTypeUtil.createStructTypeDef("serdeType",
                AtlasTypeUtil.createRequiredAttrDef(NAME, "string"),
                AtlasTypeUtil.createRequiredAttrDef("serde", "string")
        );

        AtlasEnumDef enumDef = new AtlasEnumDef("tableType", DESCRIPTION, Arrays.asList(
                new AtlasEnumDef.AtlasEnumElementDef("MANAGED", null, 1),
                new AtlasEnumDef.AtlasEnumElementDef("EXTERNAL", null, 2)
        ));

        AtlasEntityDef tblClsDef = AtlasTypeUtil
                .createClassTypeDef(HIVE_TABLE_TYPE_V2,
                        ImmutableSet.of("DataSet"),
                        AtlasTypeUtil.createOptionalAttrDef("owner", "string"),
                        AtlasTypeUtil.createOptionalAttrDef("createTime", "long"),
                        AtlasTypeUtil.createOptionalAttrDef("lastAccessTime", "date"),
                        AtlasTypeUtil.createOptionalAttrDef("temporary", "boolean"),
                        new AtlasAttributeDef("db", 
                                DATABASE_TYPE_V2,
                                true,
                                Cardinality.SINGLE,
                                0, 1, false, true, Collections.singletonList(isCompositeTargetConstraint)),
                        
                        //some tests don't set the columns field or set it to null...
                        AtlasTypeUtil.createOptionalAttrDef("columns", DataTypes.arrayTypeName(COLUMN_TYPE_V2)),
                        AtlasTypeUtil.createOptionalAttrDef("tableType", "tableType"),
                        AtlasTypeUtil.createOptionalAttrDef("serde1", "serdeType"),
                        AtlasTypeUtil.createOptionalAttrDef("serde2", "serdeType"));

        AtlasEntityDef loadProcessClsDef = AtlasTypeUtil
                .createClassTypeDef(HIVE_PROCESS_TYPE_V2,
                        ImmutableSet.of("Process"),
                        AtlasTypeUtil.createOptionalAttrDef("userName", "string"),
                        AtlasTypeUtil.createOptionalAttrDef("startTime", "int"),
                        AtlasTypeUtil.createOptionalAttrDef("endTime", "long"),
                        AtlasTypeUtil.createRequiredAttrDef("queryText", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("queryPlan", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("queryId", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("queryGraph", "string"));

        AtlasClassificationDef classificationTrait = AtlasTypeUtil
                .createTraitTypeDef("classification",ImmutableSet.<String>of(),
                        AtlasTypeUtil.createRequiredAttrDef("tag", "string"));
        AtlasClassificationDef piiTrait =
                AtlasTypeUtil.createTraitTypeDef("pii", ImmutableSet.<String>of());
        AtlasClassificationDef phiTrait =
                AtlasTypeUtil.createTraitTypeDef("phi", ImmutableSet.<String>of());
        AtlasClassificationDef pciTrait =
                AtlasTypeUtil.createTraitTypeDef("pci", ImmutableSet.<String>of());
        AtlasClassificationDef soxTrait =
                AtlasTypeUtil.createTraitTypeDef("sox", ImmutableSet.<String>of());
        AtlasClassificationDef secTrait =
                AtlasTypeUtil.createTraitTypeDef("sec", ImmutableSet.<String>of());
        AtlasClassificationDef financeTrait =
                AtlasTypeUtil.createTraitTypeDef("finance", ImmutableSet.<String>of());

        AtlasTypesDef typesDef = new AtlasTypesDef(ImmutableList.of(enumDef),
                ImmutableList.of(structTypeDef),
                ImmutableList.of(classificationTrait, piiTrait, phiTrait, pciTrait, soxTrait, secTrait, financeTrait),
                ImmutableList.of(dbClsTypeDef, columnClsDef, tblClsDef, loadProcessClsDef));

        batchCreateTypes(typesDef);
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
        //names cannot start with a digit
        return RandomStringUtils.randomAlphabetic(1) + RandomStringUtils.randomAlphanumeric(9);
    }

    protected Referenceable createHiveTableInstanceBuiltIn(String dbName, String tableName, Id dbId) throws Exception {
        Map<String, Object> values = new HashMap<>();
        values.put(NAME, dbName);
        values.put(DESCRIPTION, "foo database");
        values.put(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, dbName);
        values.put("owner", "user1");
        values.put(CLUSTER_NAME, "cl1");
        values.put("parameters", Collections.EMPTY_MAP);
        values.put("location", "/tmp");
        Referenceable databaseInstance = new Referenceable(dbId._getId(), dbId.getTypeName(), values);
        Referenceable tableInstance =
                new Referenceable(HIVE_TABLE_TYPE_BUILTIN, "classification", "pii", "phi", "pci", "sox", "sec", "finance");
        tableInstance.set(NAME, tableName);
        tableInstance.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, tableName);
        tableInstance.set("db", databaseInstance);
        tableInstance.set(DESCRIPTION, "bar table");
        tableInstance.set("lastAccessTime", "2014-07-11T08:00:00.000Z");
        tableInstance.set("type", "managed");
        tableInstance.set("level", 2);
        tableInstance.set("tableType", 1); // enum
        tableInstance.set("compressed", false);

        Struct traitInstance = (Struct) tableInstance.getTrait("classification");
        traitInstance.set("tag", "foundation_etl");

        Struct serde1Instance = new Struct("serdeType");
        serde1Instance.set(NAME, "serde1");
        serde1Instance.set("serde", "serde1");
        tableInstance.set("serde1", serde1Instance);

        Struct serde2Instance = new Struct("serdeType");
        serde2Instance.set(NAME, "serde2");
        serde2Instance.set("serde", "serde2");
        tableInstance.set("serde2", serde2Instance);

        List<String> traits = tableInstance.getTraits();
        Assert.assertEquals(traits.size(), 7);

        return tableInstance;
    }

    protected AtlasEntity createHiveTableInstanceV2(AtlasEntity databaseInstance, String tableName) throws Exception {
        AtlasEntity tableInstance = new AtlasEntity(HIVE_TABLE_TYPE_V2);
        tableInstance.setClassifications(
                Arrays.asList(new AtlasClassification("classification"),
                        new AtlasClassification("pii"),
                        new AtlasClassification("phi"),
                        new AtlasClassification("pci"),
                        new AtlasClassification("sox"),
                        new AtlasClassification("sec"),
                        new AtlasClassification("finance"))
        );

        tableInstance.setAttribute(NAME, tableName);
        tableInstance.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, tableName);
        tableInstance.setAttribute("db", databaseInstance);
        tableInstance.setAttribute(DESCRIPTION, "bar table");
        tableInstance.setAttribute("lastAccessTime", "2014-07-11T08:00:00.000Z");
        tableInstance.setAttribute("type", "managed");
        tableInstance.setAttribute("level", 2);
        tableInstance.setAttribute("tableType", 1); // enum
        tableInstance.setAttribute("compressed", false);

        AtlasClassification classification = tableInstance.getClassifications().get(0);
        classification.setAttribute("tag", "foundation_etl");

        AtlasStruct serde1Instance = new AtlasStruct("serdeType");
        serde1Instance.setAttribute(NAME, "serde1");
        serde1Instance.setAttribute("serde", "serde1");
        tableInstance.setAttribute("serde1", serde1Instance);

        AtlasStruct serde2Instance = new AtlasStruct("serdeType");
        serde2Instance.setAttribute(NAME, "serde2");
        serde2Instance.setAttribute("serde", "serde2");
        tableInstance.setAttribute("serde2", serde2Instance);

        List<AtlasClassification> traits = tableInstance.getClassifications();
        Assert.assertEquals(traits.size(), 7);

        return tableInstance;
    }
    protected Referenceable createHiveDBInstanceBuiltIn(String dbName) {
        Referenceable databaseInstance = new Referenceable(DATABASE_TYPE_BUILTIN);
        databaseInstance.set(NAME, dbName);
        databaseInstance.set(QUALIFIED_NAME, dbName);
        databaseInstance.set(CLUSTER_NAME, randomString());
        databaseInstance.set(DESCRIPTION, "foo database");
        return databaseInstance;
    }


    protected Referenceable createHiveDBInstanceV1(String dbName) {
        Referenceable databaseInstance = new Referenceable(DATABASE_TYPE);
        databaseInstance.set(NAME, dbName);
        databaseInstance.set(DESCRIPTION, "foo database");
        return databaseInstance;
    }

    protected AtlasEntity createHiveDBInstanceV2(String dbName) {
        AtlasEntity atlasEntity = new AtlasEntity(DATABASE_TYPE_V2);
        atlasEntity.setAttribute(NAME, dbName);
        atlasEntity.setAttribute(DESCRIPTION, "foo database");
        atlasEntity.setAttribute("owner", "user1");
        atlasEntity.setAttribute("locationUri", "/tmp");
        atlasEntity.setAttribute("createTime",1000);
        return atlasEntity;
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

    protected JSONArray searchByDSL(String dslQuery) throws AtlasServiceException {
        return atlasClientV1.searchByDSL(dslQuery, 10, 0);
    }
}
