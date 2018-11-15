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
package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.TestModules;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.commons.lang.time.DateUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.TestUtilsV2.ENTITY_TYPE;
import static org.apache.atlas.TestUtilsV2.ENTITY_TYPE_MAP;
import static org.apache.atlas.TestUtilsV2.ENTITY_TYPE_WITH_COMPLEX_COLLECTION_ATTR;
import static org.apache.atlas.TestUtilsV2.ENTITY_TYPE_WITH_COMPLEX_COLLECTION_ATTR_DELETE;
import static org.apache.atlas.TestUtilsV2.NAME;
import static org.apache.atlas.repository.graph.GraphHelper.getStatus;
import static org.apache.atlas.type.AtlasTypeUtil.getAtlasObjectId;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertEquals;

@Guice(modules = TestModules.TestOnlyModule.class)
public class AtlasComplexAttributesTest extends AtlasEntityTestBase {
    private AtlasEntityWithExtInfo complexCollectionAttrEntity;
    private AtlasEntityWithExtInfo complexCollectionAttrEntityForDelete;
    private AtlasEntityWithExtInfo mapAttributesEntity;

    @BeforeClass
    public void setUp() throws Exception {
        super.setUp();

        // create typeDefs
        AtlasTypesDef[] testTypesDefs = new AtlasTypesDef[] { TestUtilsV2.defineTypeWithComplexCollectionAttributes(),
                                                              TestUtilsV2.defineTypeWithMapAttributes() };
        createTypesDef(testTypesDefs);

        // create entity
        complexCollectionAttrEntity          = TestUtilsV2.createComplexCollectionAttrEntity();
        complexCollectionAttrEntityForDelete = TestUtilsV2.createComplexCollectionAttrEntity();
        mapAttributesEntity                  = TestUtilsV2.createMapAttrEntity();
    }

    @Test
    public void testCreateComplexAttributeEntity() throws Exception {
        init();

        EntityMutationResponse response      = entityStore.createOrUpdate(new AtlasEntityStream(complexCollectionAttrEntity), false);
        AtlasEntityHeader      entityCreated = response.getFirstCreatedEntityByTypeName(ENTITY_TYPE_WITH_COMPLEX_COLLECTION_ATTR);

        validateEntity(complexCollectionAttrEntity, getEntityFromStore(entityCreated));
    }

    @Test
    public void testPrimitiveMapAttributes() throws Exception {
        init();

        EntityMutationResponse response        = entityStore.createOrUpdate(new AtlasEntityStream(mapAttributesEntity), false);
        AtlasEntityHeader      entityCreated   = response.getFirstCreatedEntityByTypeName(ENTITY_TYPE_MAP);
        AtlasEntity            entityFromStore = getEntityFromStore(entityCreated);
        validateEntity(mapAttributesEntity, entityFromStore);

        // Modify map of primitives
        AtlasEntity attrEntity = getEntityFromStore(mapAttributesEntity.getEntity().getGuid());

        Map<String, String> map1 = new HashMap<String, String>() {{ put("map1Key11", "value11");
                                                                    put("map1Key22", "value22");
                                                                    put("map1Key33", "value33"); }};

        Map<String, Integer> map2 = new HashMap<String, Integer>() {{ put("map2Key11", 1100);
                                                                      put("map2Key22", 2200);
                                                                      put("map2Key33", 3300); }};

        Map<String, Boolean> map3 = new HashMap<String, Boolean>() {{ put("map3Key11", true);
                                                                      put("map3Key22", false);
                                                                      put("map3Key33", true); }};

        Map<String, Float> map4 = new HashMap<String, Float>() {{ put("map4Key11", 11.0f);
                                                                  put("map4Key22", 22.0f);
                                                                  put("map4Key33", 33.0f); }};

        Map<String, Date> map5 = new HashMap<String, Date>() {{ put("map5Key11", DateUtils.addHours(new Date(), 1));
                                                                put("map5Key22", DateUtils.addHours(new Date(), 2));
                                                                put("map5Key33", DateUtils.addHours(new Date(), 3)); }};

        updateEntityMapAttributes(attrEntity, map1, map2, map3, map4, map5);

        AtlasEntitiesWithExtInfo attrEntitiesInfo = new AtlasEntitiesWithExtInfo(attrEntity);
        response = entityStore.createOrUpdate(new AtlasEntityStream(attrEntitiesInfo), false);
        AtlasEntityHeader updatedAttrEntity = response.getFirstUpdatedEntityByTypeName(ENTITY_TYPE_MAP);
        AtlasEntity       updatedFromStore  = getEntityFromStore(updatedAttrEntity);
        validateEntity(attrEntitiesInfo, updatedFromStore);

        // Add new entry to map of primitives
        map1.put("map1Key44", "value44");
        map2.put("map2Key44", 4400);
        map3.put("map3Key44", false);
        map4.put("map4Key44", 44.0f);
        map5.put("map5Key44", DateUtils.addHours(new Date(), 4));

        updateEntityMapAttributes(attrEntity, map1, map2, map3, map4, map5);

        attrEntitiesInfo  = new AtlasEntitiesWithExtInfo(attrEntity);
        response          = entityStore.createOrUpdate(new AtlasEntityStream(attrEntitiesInfo), false);
        updatedAttrEntity = response.getFirstUpdatedEntityByTypeName(ENTITY_TYPE_MAP);
        updatedFromStore  = getEntityFromStore(updatedAttrEntity);
        validateEntity(attrEntitiesInfo, updatedFromStore);

        // Remove an entry from map of primitives
        map1.remove("map1Key11");
        map2.remove("map2Key11");
        map3.remove("map3Key11");
        map4.remove("map4Key11");
        map5.remove("map5Key11");

        updateEntityMapAttributes(attrEntity, map1, map2, map3, map4, map5);

        attrEntitiesInfo  = new AtlasEntitiesWithExtInfo(attrEntity);
        response          = entityStore.createOrUpdate(new AtlasEntityStream(attrEntitiesInfo), false);
        updatedAttrEntity = response.getFirstUpdatedEntityByTypeName(ENTITY_TYPE_MAP);
        updatedFromStore  = getEntityFromStore(updatedAttrEntity);
        validateEntity(attrEntitiesInfo, updatedFromStore);

        // Edit existing entry to map of primitives
        map1.put("map1Key44", "value44-edit");
        map2.put("map2Key44", 5555);
        map3.put("map3Key44", true);
        map4.put("map4Key44", 55.5f);
        map5.put("map5Key44", DateUtils.addHours(new Date(), 5));

        updateEntityMapAttributes(attrEntity, map1, map2, map3, map4, map5);

        attrEntitiesInfo  = new AtlasEntitiesWithExtInfo(attrEntity);
        response          = entityStore.createOrUpdate(new AtlasEntityStream(attrEntitiesInfo), false);
        updatedAttrEntity = response.getFirstUpdatedEntityByTypeName(ENTITY_TYPE_MAP);
        updatedFromStore  = getEntityFromStore(updatedAttrEntity);
        validateEntity(attrEntitiesInfo, updatedFromStore);

        // clear primitive map entries
        map1.clear();
        map2.clear();
        map3.clear();
        map4.clear();
        map5.clear();

        updateEntityMapAttributes(attrEntity, map1, map2, map3, map4, map5);

        attrEntitiesInfo  = new AtlasEntitiesWithExtInfo(attrEntity);
        response          = entityStore.createOrUpdate(new AtlasEntityStream(attrEntitiesInfo), false);
        updatedAttrEntity = response.getFirstUpdatedEntityByTypeName(ENTITY_TYPE_MAP);
        updatedFromStore  = getEntityFromStore(updatedAttrEntity);
        validateEntity(attrEntitiesInfo, updatedFromStore);
    }

    private void updateEntityMapAttributes(AtlasEntity attrEntity, Map<String, String> map1, Map<String, Integer> map2,
                                           Map<String, Boolean> map3, Map<String, Float> map4, Map<String, Date> map5) {
        attrEntity.setAttribute("mapAttr1", map1);
        attrEntity.setAttribute("mapAttr2", map2);
        attrEntity.setAttribute("mapAttr3", map3);
        attrEntity.setAttribute("mapAttr4", map4);
        attrEntity.setAttribute("mapAttr5", map5);
    }

    @Test(dependsOnMethods = "testCreateComplexAttributeEntity")
    public void testStructArray() throws Exception {
        init();
        AtlasEntity              complexEntity       = getEntityFromStore(complexCollectionAttrEntity.getEntity().getGuid());
        AtlasEntitiesWithExtInfo complexEntitiesInfo = new AtlasEntitiesWithExtInfo(complexEntity);

        // Modify array of structs
        List<AtlasStruct> structList = new ArrayList<>(Arrays.asList(new AtlasStruct("struct_type", "name", "structArray00"),
                                                                     new AtlasStruct("struct_type", "name", "structArray11"),
                                                                     new AtlasStruct("struct_type", "name", "structArray22")));
        complexEntity.setAttribute("listOfStructs", structList);

        EntityMutationResponse response             = entityStore.createOrUpdate(new AtlasEntityStream(complexEntitiesInfo), false);
        AtlasEntityHeader      updatedComplexEntity = response.getFirstUpdatedEntityByTypeName(ENTITY_TYPE_WITH_COMPLEX_COLLECTION_ATTR);
        validateEntity(complexEntitiesInfo, getEntityFromStore(updatedComplexEntity));

        // add a new element to array of struct
        init();
        structList.add(new AtlasStruct("struct_type", "name", "structArray33"));
        complexEntity.setAttribute("listOfStructs", structList);
        response = entityStore.createOrUpdate(new AtlasEntityStream(complexEntitiesInfo), false);
        updatedComplexEntity = response.getFirstUpdatedEntityByTypeName(ENTITY_TYPE_WITH_COMPLEX_COLLECTION_ATTR);
        validateEntity(complexEntitiesInfo, getEntityFromStore(updatedComplexEntity));

        // remove one of the struct values - structArray00
        init();
        structList.remove(0);
        complexEntity.setAttribute("listOfStructs", structList);
        response = entityStore.createOrUpdate(new AtlasEntityStream(complexEntitiesInfo), false);
        updatedComplexEntity = response.getFirstUpdatedEntityByTypeName(ENTITY_TYPE_WITH_COMPLEX_COLLECTION_ATTR);
        validateEntity(complexEntitiesInfo, getEntityFromStore(updatedComplexEntity));

        // Update struct value within array of struct
        init();
        structList.get(0).setAttribute(NAME, "structArray11-edit");
        complexEntity.setAttribute("listOfStructs", structList);
        response = entityStore.createOrUpdate(new AtlasEntityStream(complexEntitiesInfo), false);
        updatedComplexEntity = response.getFirstUpdatedEntityByTypeName(ENTITY_TYPE_WITH_COMPLEX_COLLECTION_ATTR);
        validateEntity(complexEntitiesInfo, getEntityFromStore(updatedComplexEntity));

        // add a repeated element to array of struct
        init();
        structList.add(new AtlasStruct("struct_type", "name", "structArray33"));
        complexEntity.setAttribute("listOfStructs", structList);
        response = entityStore.createOrUpdate(new AtlasEntityStream(complexEntitiesInfo), false);
        updatedComplexEntity = response.getFirstUpdatedEntityByTypeName(ENTITY_TYPE_WITH_COMPLEX_COLLECTION_ATTR);
        validateEntity(complexEntitiesInfo, getEntityFromStore(updatedComplexEntity));

        // Remove all elements. Should set array attribute to null
        init();
        structList.clear();
        complexEntity.setAttribute("listOfStructs", structList);
        response = entityStore.createOrUpdate(new AtlasEntityStream(complexEntitiesInfo), false);
        updatedComplexEntity = response.getFirstUpdatedEntityByTypeName(ENTITY_TYPE_WITH_COMPLEX_COLLECTION_ATTR);
        validateEntity(complexEntitiesInfo, getEntityFromStore(updatedComplexEntity));
    }

    @Test(dependsOnMethods = "testStructArray")
    public void testEntityArray() throws Exception {
        init();
        AtlasEntity              complexEntity       = getEntityFromStore(complexCollectionAttrEntity.getEntity().getGuid());
        AtlasEntitiesWithExtInfo complexEntitiesInfo = new AtlasEntitiesWithExtInfo(complexEntity);
        AtlasEntityType          entityType          = typeRegistry.getEntityTypeByName(ENTITY_TYPE);

        // Replace list of entities with new values
        AtlasEntity e0Array = new AtlasEntity(ENTITY_TYPE, new HashMap<String, Object>() {{ put(NAME, "entityArray00"); put("isReplicated", true); }});
        AtlasEntity e1Array = new AtlasEntity(ENTITY_TYPE, new HashMap<String, Object>() {{ put(NAME, "entityArray11"); put("isReplicated", false); }});
        AtlasEntity e2Array = new AtlasEntity(ENTITY_TYPE, new HashMap<String, Object>() {{ put(NAME, "entityArray22"); put("isReplicated", true); }});

        List<AtlasObjectId> entityList = new ArrayList<>(Arrays.asList(getAtlasObjectId(e0Array), getAtlasObjectId(e1Array), getAtlasObjectId(e2Array)));

        complexEntity.setAttribute("listOfEntities", entityList);
        complexEntitiesInfo.addReferredEntity(e0Array);
        complexEntitiesInfo.addReferredEntity(e1Array);
        complexEntitiesInfo.addReferredEntity(e2Array);

        init();
        EntityMutationResponse response             = entityStore.createOrUpdate(new AtlasEntityStream(complexEntitiesInfo), false);
        AtlasEntityHeader      updatedComplexEntity = response.getFirstUpdatedEntityByTypeName(ENTITY_TYPE_WITH_COMPLEX_COLLECTION_ATTR);
        validateEntity(complexEntitiesInfo, getEntityFromStore(updatedComplexEntity));

        // add a new element to list of entities
        init();

        e0Array = entityStore.getByUniqueAttributes(entityType, new HashMap<String, Object>() {{ put(NAME, "entityArray00"); put("isReplicated", true); }}).getEntity();
        e1Array = entityStore.getByUniqueAttributes(entityType, new HashMap<String, Object>() {{ put(NAME, "entityArray11"); put("isReplicated", false); }}).getEntity();
        e2Array = entityStore.getByUniqueAttributes(entityType, new HashMap<String, Object>() {{ put(NAME, "entityArray22"); put("isReplicated", true); }}).getEntity();
        AtlasEntity e3Array = new AtlasEntity(ENTITY_TYPE, new HashMap<String, Object>() {{ put(NAME, "entityArray33"); put("isReplicated", true); }});

        entityList = new ArrayList<>(Arrays.asList(getAtlasObjectId(e0Array), getAtlasObjectId(e1Array), getAtlasObjectId(e2Array), getAtlasObjectId(e3Array)));

        complexEntity.setAttribute("listOfEntities", entityList);
        complexEntitiesInfo.getReferredEntities().clear();
        complexEntitiesInfo.addReferredEntity(e3Array);

        response = entityStore.createOrUpdate(new AtlasEntityStream(complexEntitiesInfo), false);
        updatedComplexEntity = response.getFirstUpdatedEntityByTypeName(ENTITY_TYPE_WITH_COMPLEX_COLLECTION_ATTR);
        validateEntity(complexEntitiesInfo, getEntityFromStore(updatedComplexEntity));

        // remove one of the entity values - entityArray00
        init();
        e3Array = entityStore.getByUniqueAttributes(entityType, new HashMap<String, Object>() {{ put(NAME, "entityArray33"); put("isReplicated", true); }}).getEntity();
        entityList = new ArrayList<>(Arrays.asList(getAtlasObjectId(e1Array), getAtlasObjectId(e2Array), getAtlasObjectId(e3Array)));
        complexEntity.setAttribute("listOfEntities", entityList);
        complexEntitiesInfo.getReferredEntities().clear();

        response = entityStore.createOrUpdate(new AtlasEntityStream(complexEntitiesInfo), false);
        updatedComplexEntity = response.getFirstUpdatedEntityByTypeName(ENTITY_TYPE_WITH_COMPLEX_COLLECTION_ATTR);
        validateEntity(complexEntitiesInfo, getEntityFromStore(updatedComplexEntity));

        // Update entity value within array of struct
        init();
        e1Array.setAttribute(NAME, "entityArray11-edit");
        complexEntity.setAttribute("listOfEntities", entityList);

        response = entityStore.createOrUpdate(new AtlasEntityStream(complexEntitiesInfo), false);
        updatedComplexEntity = response.getFirstUpdatedEntityByTypeName(ENTITY_TYPE_WITH_COMPLEX_COLLECTION_ATTR);
        validateEntity(complexEntitiesInfo, getEntityFromStore(updatedComplexEntity));

        // add a repeated element to array of struct
        init();
        AtlasEntity e3Array_duplicate = new AtlasEntity(ENTITY_TYPE, new HashMap<String, Object>() {{ put(NAME, "entityArray33"); put("isReplicated", true); }});
        entityList.add(getAtlasObjectId(e3Array_duplicate));
        complexEntity.setAttribute("listOfEntities", entityList);
        complexEntitiesInfo.getReferredEntities().clear();
        complexEntitiesInfo.addReferredEntity(e3Array_duplicate);

        response = entityStore.createOrUpdate(new AtlasEntityStream(complexEntitiesInfo), false);
        updatedComplexEntity = response.getFirstUpdatedEntityByTypeName(ENTITY_TYPE_WITH_COMPLEX_COLLECTION_ATTR);
        validateEntity(complexEntitiesInfo, getEntityFromStore(updatedComplexEntity));

        // Remove all elements. Should set array attribute to null
        init();
        entityList.clear();
        complexEntity.setAttribute("listOfEntities", entityList);
        complexEntitiesInfo.getReferredEntities().clear();

        response = entityStore.createOrUpdate(new AtlasEntityStream(complexEntitiesInfo), false);
        updatedComplexEntity = response.getFirstUpdatedEntityByTypeName(ENTITY_TYPE_WITH_COMPLEX_COLLECTION_ATTR);
        validateEntity(complexEntitiesInfo, getEntityFromStore(updatedComplexEntity));
    }

    @Test(dependsOnMethods = "testEntityArray")
    public void testStructMap() throws Exception {
        init();
        AtlasEntity              complexEntity       = getEntityFromStore(complexCollectionAttrEntity.getEntity().getGuid());
        AtlasEntitiesWithExtInfo complexEntitiesInfo = new AtlasEntitiesWithExtInfo(complexEntity);

        // Modify map of structs
        HashMap<String, AtlasStruct> structMap = new HashMap<String, AtlasStruct>() {{
                                                        put("key00", new AtlasStruct("struct_type", "name", "structMap00"));
                                                        put("key11", new AtlasStruct("struct_type", "name", "structMap11"));
                                                        put("key22", new AtlasStruct("struct_type", "name", "structMap22")); }};

        complexEntity.setAttribute("mapOfStructs", structMap);

        EntityMutationResponse response             = entityStore.createOrUpdate(new AtlasEntityStream(complexEntitiesInfo), false);
        AtlasEntityHeader      updatedComplexEntity = response.getFirstUpdatedEntityByTypeName(ENTITY_TYPE_WITH_COMPLEX_COLLECTION_ATTR);
        validateEntity(complexEntitiesInfo, getEntityFromStore(updatedComplexEntity));

        // add a new element to map of struct - structMap6
        init();
        structMap.put("key33", new AtlasStruct("struct_type", "name", "structMap33"));
        complexEntity.setAttribute("mapOfStructs", structMap);

        response             = entityStore.createOrUpdate(new AtlasEntityStream(complexEntitiesInfo), false);
        updatedComplexEntity = response.getFirstUpdatedEntityByTypeName(ENTITY_TYPE_WITH_COMPLEX_COLLECTION_ATTR);
        validateEntity(complexEntitiesInfo, getEntityFromStore(updatedComplexEntity));

        // remove one of the entity values - structMap
        init();
        structMap.remove("key00");
        complexEntity.setAttribute("mapOfStructs", structMap);

        response             = entityStore.createOrUpdate(new AtlasEntityStream(complexEntitiesInfo), false);
        updatedComplexEntity = response.getFirstUpdatedEntityByTypeName(ENTITY_TYPE_WITH_COMPLEX_COLLECTION_ATTR);
        validateEntity(complexEntitiesInfo, getEntityFromStore(updatedComplexEntity));

        // Update struct value within map of struct
        init();
        structMap.get("key11").setAttribute("name", "structMap11-edit");
        complexEntity.setAttribute("mapOfStructs", structMap);

        response = entityStore.createOrUpdate(new AtlasEntityStream(complexEntitiesInfo), false);
        updatedComplexEntity = response.getFirstUpdatedEntityByTypeName(ENTITY_TYPE_WITH_COMPLEX_COLLECTION_ATTR);
        validateEntity(complexEntitiesInfo, getEntityFromStore(updatedComplexEntity));

        // add a repeated element to array of struct
        init();
        structMap.put("key33", new AtlasStruct("struct_type", "name", "structMap33"));
        complexEntity.setAttribute("mapOfStructs", structMap);
        response = entityStore.createOrUpdate(new AtlasEntityStream(complexEntitiesInfo), false);
        updatedComplexEntity = response.getFirstUpdatedEntityByTypeName(ENTITY_TYPE_WITH_COMPLEX_COLLECTION_ATTR);
        // no update since duplicate entry
        assertNull(updatedComplexEntity);

        // Remove all elements. Should set array attribute to null
        init();
        structMap.clear();
        complexEntity.setAttribute("mapOfStructs", structMap);
        response = entityStore.createOrUpdate(new AtlasEntityStream(complexEntitiesInfo), false);
        updatedComplexEntity = response.getFirstUpdatedEntityByTypeName(ENTITY_TYPE_WITH_COMPLEX_COLLECTION_ATTR);
        validateEntity(complexEntitiesInfo, getEntityFromStore(updatedComplexEntity));
    }

    @Test(dependsOnMethods = "testStructMap")
    public void testEntityMap() throws Exception {
        init();
        AtlasEntity              complexEntity       = getEntityFromStore(complexCollectionAttrEntity.getEntity().getGuid());
        AtlasEntitiesWithExtInfo complexEntitiesInfo = new AtlasEntitiesWithExtInfo(complexEntity);

        // Modify map of entities
        AtlasEntity e0MapValue = new AtlasEntity(ENTITY_TYPE, new HashMap<String, Object>() {{ put(NAME, "entityMapValue00"); put("isReplicated", false); }});
        AtlasEntity e1MapValue = new AtlasEntity(ENTITY_TYPE, new HashMap<String, Object>() {{ put(NAME, "entityMapValue11"); put("isReplicated", true); }});
        AtlasEntity e2MapValue = new AtlasEntity(ENTITY_TYPE, new HashMap<String, Object>() {{ put(NAME, "entityMapValue22"); put("isReplicated", false); }});

        HashMap<String, Object> entityMap = new HashMap<String, Object>() {{ put("key00", getAtlasObjectId(e0MapValue));
                                                                             put("key11", getAtlasObjectId(e1MapValue));
                                                                             put("key22", getAtlasObjectId(e2MapValue)); }};
        complexEntity.setAttribute("mapOfEntities", entityMap);
        complexEntitiesInfo.addReferredEntity(e0MapValue);
        complexEntitiesInfo.addReferredEntity(e1MapValue);
        complexEntitiesInfo.addReferredEntity(e2MapValue);

        init();
        EntityMutationResponse response             = entityStore.createOrUpdate(new AtlasEntityStream(complexEntitiesInfo), false);
        AtlasEntityHeader      updatedComplexEntity = response.getFirstUpdatedEntityByTypeName(ENTITY_TYPE_WITH_COMPLEX_COLLECTION_ATTR);
        validateEntity(complexEntitiesInfo, getEntityFromStore(updatedComplexEntity));

        // add a new element to map of entities
        init();
        AtlasEntity e3MapValue = new AtlasEntity(ENTITY_TYPE, new HashMap<String, Object>() {{ put(NAME, "entityMapValue33"); put("isReplicated", false); }});
        entityMap.put("key33", getAtlasObjectId(e3MapValue));
        complexEntity.setAttribute("mapOfEntities", entityMap);
        complexEntitiesInfo.addReferredEntity(e3MapValue);

        response = entityStore.createOrUpdate(new AtlasEntityStream(complexEntitiesInfo), false);
        updatedComplexEntity = response.getFirstUpdatedEntityByTypeName(ENTITY_TYPE_WITH_COMPLEX_COLLECTION_ATTR);
        validateEntity(complexEntitiesInfo, getEntityFromStore(updatedComplexEntity));

        // remove one of the entity values - [key00 : entityMapValue00]
        init();
        entityMap.remove("key00");
        complexEntity.setAttribute("mapOfEntities", entityMap);
        complexEntitiesInfo.addReferredEntity(e3MapValue);

        response = entityStore.createOrUpdate(new AtlasEntityStream(complexEntitiesInfo), false);
        updatedComplexEntity = response.getFirstUpdatedEntityByTypeName(ENTITY_TYPE_WITH_COMPLEX_COLLECTION_ATTR);
        validateEntity(complexEntitiesInfo, getEntityFromStore(updatedComplexEntity));

        // Update entity value within map of entities
        init();

        AtlasEntity e1MapValueEdit = new AtlasEntity(ENTITY_TYPE, new HashMap<String, Object>() {{ put(NAME, "entityMapValue11-edit"); put("isReplicated", false); }});
        entityMap.clear();
        entityMap.put("key11", getAtlasObjectId(e1MapValueEdit));
        entityMap.put("key22", getAtlasObjectId(e2MapValue));
        entityMap.put("key33", getAtlasObjectId(e3MapValue));
        complexEntity.setAttribute("mapOfEntities", entityMap);
        complexEntitiesInfo.addReferredEntity(e1MapValueEdit);

        response = entityStore.createOrUpdate(new AtlasEntityStream(complexEntitiesInfo), false);
        updatedComplexEntity = response.getFirstUpdatedEntityByTypeName(ENTITY_TYPE_WITH_COMPLEX_COLLECTION_ATTR);
        validateEntity(complexEntitiesInfo, getEntityFromStore(updatedComplexEntity));

        // add a repeated element to map of entities
        init();
        e3MapValue = new AtlasEntity(ENTITY_TYPE, new HashMap<String, Object>() {{ put(NAME, "entityMapValue33"); put("isReplicated", false); }});
        entityMap.put("key33", getAtlasObjectId(e3MapValue));
        complexEntity.setAttribute("mapOfEntities", entityMap);
        complexEntitiesInfo.addReferredEntity(e3MapValue);

        response = entityStore.createOrUpdate(new AtlasEntityStream(complexEntitiesInfo), false);
        updatedComplexEntity = response.getFirstUpdatedEntityByTypeName(ENTITY_TYPE_WITH_COMPLEX_COLLECTION_ATTR);
        validateEntity(complexEntitiesInfo, getEntityFromStore(updatedComplexEntity));

        // Remove all elements. Should set map attribute to null
        init();
        entityMap.clear();
        complexEntity.setAttribute("mapOfEntities", entityMap);
        complexEntitiesInfo.addReferredEntity(e3MapValue);

        response = entityStore.createOrUpdate(new AtlasEntityStream(complexEntitiesInfo), false);
        updatedComplexEntity = response.getFirstUpdatedEntityByTypeName(ENTITY_TYPE_WITH_COMPLEX_COLLECTION_ATTR);
        validateEntity(complexEntitiesInfo, getEntityFromStore(updatedComplexEntity));
    }

    @Test(dependsOnMethods = "testEntityMap")
    public void testDeleteEntityRemoveReferences() throws Exception {
        init();

        complexCollectionAttrEntityForDelete.getEntity().setAttribute(NAME, ENTITY_TYPE_WITH_COMPLEX_COLLECTION_ATTR_DELETE);

        EntityMutationResponse response      = entityStore.createOrUpdate(new AtlasEntityStream(complexCollectionAttrEntityForDelete), false);
        AtlasEntityHeader      entityCreated = response.getFirstCreatedEntityByTypeName(ENTITY_TYPE_WITH_COMPLEX_COLLECTION_ATTR);

        validateEntity(complexCollectionAttrEntityForDelete, getEntityFromStore(entityCreated));

        // delete entity and check if referenced complex attribute edges are deleted
        response = entityStore.deleteById(entityCreated.getGuid());

        AtlasEntityHeader entityDeleted = response.getFirstDeletedEntityByTypeName(ENTITY_TYPE_WITH_COMPLEX_COLLECTION_ATTR);

        AtlasEntityWithExtInfo deletedEntityWithExtInfo = entityStore.getById(entityDeleted.getGuid());
        AtlasVertex            deletedEntityVertex      = AtlasGraphUtilsV2.findByGuid(entityDeleted.getGuid());
        Iterator<AtlasEdge>    edges                    = deletedEntityVertex.getEdges(AtlasEdgeDirection.OUT).iterator();

        // validate all attribute edges are deleted
        while (edges != null && edges.hasNext()) {
            assertEquals(getStatus(edges.next()), AtlasEntity.Status.DELETED);
        }

        AtlasEntity                deletedEntity  = deletedEntityWithExtInfo.getEntity();
        List<AtlasObjectId>        listOfEntities = (List<AtlasObjectId>) deletedEntity.getAttribute("listOfEntities");
        Map<String, AtlasObjectId> mapOfEntities  = (Map<String, AtlasObjectId>) deletedEntity.getAttribute("mapOfEntities");

        // validate entity attributes are deleted
        for (AtlasObjectId o  : listOfEntities) {
            AtlasEntity entity = deletedEntityWithExtInfo.getEntity(o.getGuid());
            assertEquals(entity.getStatus(), AtlasEntity.Status.DELETED);
        }

        for (AtlasObjectId o  : mapOfEntities.values()) {
            AtlasEntity entity = deletedEntityWithExtInfo.getEntity(o.getGuid());
            assertEquals(entity.getStatus(), AtlasEntity.Status.DELETED);
        }
    }
}