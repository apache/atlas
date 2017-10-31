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

package org.apache.atlas.web.integration;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.Cardinality;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.notification.NotificationConsumer;
import org.apache.atlas.kafka.*;
import org.apache.atlas.notification.entity.EntityNotification;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.*;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.apache.atlas.utils.AuthenticationUtil;
import org.apache.atlas.utils.ParamChecker;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.RandomStringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef.CONSTRAINT_PARAM_ATTRIBUTE;
import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef.CONSTRAINT_TYPE_INVERSE_REF;
import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef.CONSTRAINT_TYPE_OWNED_REF;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Base class for integration tests.
 * Sets up the web resource and has helper methods to created type and entity.
 */
public abstract class BaseResourceIT {

    public static final String ATLAS_REST_ADDRESS = "atlas.rest.address";
    public static final String NAME = "name";
    public static final String QUALIFIED_NAME = "qualifiedName";
    public static final String CLUSTER_NAME = "clusterName";
    public static final String DESCRIPTION = "description";
    public static final String PII_TAG = "pii_Tag";
    public static final String PHI_TAG = "phi_Tag";
    public static final String PCI_TAG = "pci_Tag";
    public static final String SOX_TAG = "sox_Tag";
    public static final String SEC_TAG = "sec_Tag";
    public static final String FINANCE_TAG = "finance_Tag";
    public static final String CLASSIFICATION = "classification";

    // All service clients
    protected AtlasClient atlasClientV1;
    protected AtlasClientV2 atlasClientV2;

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
            atlasClientV2 = new AtlasClientV2(atlasUrls, new String[]{"admin", "admin"});
        } else {
            atlasClientV1 = new AtlasClient(atlasUrls);
            atlasClientV2 = new AtlasClientV2(atlasUrls);
        }
    }

    protected void batchCreateTypes(AtlasTypesDef typesDef) throws AtlasServiceException {
        AtlasTypesDef toCreate = new AtlasTypesDef();
        for (AtlasEnumDef enumDef : typesDef.getEnumDefs()) {
            if (atlasClientV2.typeWithNameExists(enumDef.getName())) {
                LOG.warn("Type with name {} already exists. Skipping", enumDef.getName());
            } else {
                toCreate.getEnumDefs().add(enumDef);
            }
        }

        for (AtlasStructDef structDef : typesDef.getStructDefs()) {
            if (atlasClientV2.typeWithNameExists(structDef.getName())) {
                LOG.warn("Type with name {} already exists. Skipping", structDef.getName());
            } else {
                toCreate.getStructDefs().add(structDef);
            }
        }

        for (AtlasEntityDef entityDef : typesDef.getEntityDefs()) {
            if (atlasClientV2.typeWithNameExists(entityDef.getName())) {
                LOG.warn("Type with name {} already exists. Skipping", entityDef.getName());
            } else  {
                toCreate.getEntityDefs().add(entityDef);
            }
        }

        for (AtlasClassificationDef classificationDef : typesDef.getClassificationDefs()) {
            if (atlasClientV2.typeWithNameExists(classificationDef.getName())) {
                LOG.warn("Type with name {} already exists. Skipping", classificationDef.getName());
            } else  {
                toCreate.getClassificationDefs().add(classificationDef);
            }
        }

        atlasClientV2.createAtlasTypeDefs(toCreate);
    }

    protected void createType(AtlasTypesDef typesDef) throws AtlasServiceException {
        // Since the bulk create bails out on a single failure, this has to be done as a workaround
        batchCreateTypes(typesDef);
    }

    protected List<String> createType(TypesDef typesDef) throws Exception {
        List<EnumTypeDefinition> enumTypes = new ArrayList<>();
        List<StructTypeDefinition> structTypes = new ArrayList<>();
        List<HierarchicalTypeDefinition<TraitType>> traitTypes = new ArrayList<>();
        List<HierarchicalTypeDefinition<ClassType>> classTypes = new ArrayList<>();

        for (EnumTypeDefinition enumTypeDefinition : typesDef.enumTypesAsJavaList()) {
            if (atlasClientV2.typeWithNameExists(enumTypeDefinition.name)) {
                LOG.warn("Type with name {} already exists. Skipping", enumTypeDefinition.name);
            } else {
                enumTypes.add(enumTypeDefinition);
            }
        }
        for (StructTypeDefinition structTypeDefinition : typesDef.structTypesAsJavaList()) {
            if (atlasClientV2.typeWithNameExists(structTypeDefinition.typeName)) {
                LOG.warn("Type with name {} already exists. Skipping", structTypeDefinition.typeName);
            } else {
                structTypes.add(structTypeDefinition);
            }
        }
        for (HierarchicalTypeDefinition<TraitType> hierarchicalTypeDefinition : typesDef.traitTypesAsJavaList()) {
            if (atlasClientV2.typeWithNameExists(hierarchicalTypeDefinition.typeName)) {
                LOG.warn("Type with name {} already exists. Skipping", hierarchicalTypeDefinition.typeName);
            } else {
                traitTypes.add(hierarchicalTypeDefinition);
            }
        }
        for (HierarchicalTypeDefinition<ClassType> hierarchicalTypeDefinition : typesDef.classTypesAsJavaList()) {
            if (atlasClientV2.typeWithNameExists(hierarchicalTypeDefinition.typeName)) {
                LOG.warn("Type with name {} already exists. Skipping", hierarchicalTypeDefinition.typeName);
            } else {
                classTypes.add(hierarchicalTypeDefinition);
            }
        }

        TypesDef toCreate = TypesUtil.getTypesDef(ImmutableList.copyOf(enumTypes),
                ImmutableList.copyOf(structTypes),
                ImmutableList.copyOf(traitTypes),
                ImmutableList.copyOf(classTypes));
        return atlasClientV1.createType(toCreate);
    }

    protected List<String> createType(String typesAsJSON) throws Exception {
        return createType(TypesSerialization.fromJson(typesAsJSON));
    }

    protected Id createInstance(Referenceable referenceable) throws Exception {
        String typeName = referenceable.getTypeName();
        System.out.println("creating instance of type " + typeName);

        List<String> guids = atlasClientV1.createEntity(referenceable);
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
                entity = atlasClientV2.createEntity(new AtlasEntityWithExtInfo(atlasEntity));
                assertNotNull(entity);
                assertNotNull(entity.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE));
                assertTrue(entity.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE).size() > 0);
                return entity.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE).get(0);
            } else {
                entity = atlasClientV2.updateEntity(new AtlasEntityWithExtInfo(atlasEntity));
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
                TypesUtil.createTraitTypeDef(PII_TAG, ImmutableSet.<String>of());
        HierarchicalTypeDefinition<TraitType> phiTrait =
                TypesUtil.createTraitTypeDef(PHI_TAG, ImmutableSet.<String>of());
        HierarchicalTypeDefinition<TraitType> pciTrait =
                TypesUtil.createTraitTypeDef(PCI_TAG, ImmutableSet.<String>of());
        HierarchicalTypeDefinition<TraitType> soxTrait =
                TypesUtil.createTraitTypeDef(SOX_TAG, ImmutableSet.<String>of());
        HierarchicalTypeDefinition<TraitType> secTrait =
                TypesUtil.createTraitTypeDef(SEC_TAG, ImmutableSet.<String>of());
        HierarchicalTypeDefinition<TraitType> financeTrait =
                TypesUtil.createTraitTypeDef(FINANCE_TAG, ImmutableSet.<String>of());
        HierarchicalTypeDefinition<TraitType> factTrait =
                TypesUtil.createTraitTypeDef("Fact" + randomString(), ImmutableSet.<String>of());
        HierarchicalTypeDefinition<TraitType> etlTrait =
                TypesUtil.createTraitTypeDef("ETL" + randomString(), ImmutableSet.<String>of());
        HierarchicalTypeDefinition<TraitType> dimensionTrait =
                TypesUtil.createTraitTypeDef("Dimension" + randomString(), ImmutableSet.<String>of());
        HierarchicalTypeDefinition<TraitType> metricTrait =
                TypesUtil.createTraitTypeDef("Metric" + randomString(), ImmutableSet.<String>of());

        createType(getTypesDef(ImmutableList.of(enumTypeDefinition), ImmutableList.of(structTypeDefinition),
                ImmutableList.of(classificationTrait, piiTrait, phiTrait, pciTrait,
                        soxTrait, secTrait, financeTrait, factTrait, etlTrait, dimensionTrait, metricTrait),
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
                AtlasTypeUtil.createTraitTypeDef(PII_TAG, ImmutableSet.<String>of());
        AtlasClassificationDef phiTrait =
                AtlasTypeUtil.createTraitTypeDef(PHI_TAG, ImmutableSet.<String>of());
        AtlasClassificationDef pciTrait =
                AtlasTypeUtil.createTraitTypeDef(PCI_TAG, ImmutableSet.<String>of());
        AtlasClassificationDef soxTrait =
                AtlasTypeUtil.createTraitTypeDef(SOX_TAG, ImmutableSet.<String>of());
        AtlasClassificationDef secTrait =
                AtlasTypeUtil.createTraitTypeDef(SEC_TAG, ImmutableSet.<String>of());
        AtlasClassificationDef financeTrait =
                AtlasTypeUtil.createTraitTypeDef(FINANCE_TAG, ImmutableSet.<String>of());

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
                new Referenceable(HIVE_TABLE_TYPE_BUILTIN, CLASSIFICATION, PII_TAG, PHI_TAG, PCI_TAG, SOX_TAG, SEC_TAG, FINANCE_TAG);
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
                Arrays.asList(new AtlasClassification(CLASSIFICATION),
                        new AtlasClassification(PII_TAG),
                        new AtlasClassification(PHI_TAG),
                        new AtlasClassification(PCI_TAG),
                        new AtlasClassification(SOX_TAG),
                        new AtlasClassification(SEC_TAG),
                        new AtlasClassification(FINANCE_TAG))
        );

        tableInstance.setAttribute(NAME, tableName);
        tableInstance.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, tableName);
        tableInstance.setAttribute("db", AtlasTypeUtil.getAtlasObjectId(databaseInstance));
        tableInstance.setAttribute(DESCRIPTION, "bar table");
        tableInstance.setAttribute("lastAccessTime", "2014-07-11T08:00:00.000Z");
        tableInstance.setAttribute("type", "managed");
        tableInstance.setAttribute("level", 2);
        tableInstance.setAttribute("tableType", "MANAGED"); // enum
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
        databaseInstance.set(CLUSTER_NAME, "fooCluster");
        return databaseInstance;
    }

    protected AtlasEntity createHiveDBInstanceV2(String dbName) {
        AtlasEntity atlasEntity = new AtlasEntity(DATABASE_TYPE_V2);
        atlasEntity.setAttribute(NAME, dbName);
        atlasEntity.setAttribute(DESCRIPTION, "foo database");
        atlasEntity.setAttribute(CLUSTER_NAME, "fooCluster");
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

                    while (System.currentTimeMillis() < maxCurrentTime) {
                        List<AtlasKafkaMessage<EntityNotification>> messageList = consumer.receive();
                            if(messageList.size() > 0) {
                                EntityNotification notification = messageList.get(0).getMessage();
                                if (predicate.evaluate(notification)) {
                                    pair.left = notification;
                                    return true;
                                }
                            }else{
                                LOG.info( System.currentTimeMillis()+ " messageList no records" +maxCurrentTime );
                            }
                    }
                } catch(Exception e) {
                    LOG.error(" waitForNotification", e);
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
