/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.omrs.archivemanager.store;

import org.apache.atlas.omrs.archivemanager.OMRSArchiveBuilder;
import org.apache.atlas.omrs.archivemanager.properties.OpenMetadataArchive;
import org.apache.atlas.omrs.archivemanager.properties.OpenMetadataArchiveType;
import org.apache.atlas.omrs.ffdc.exception.OMRSLogicErrorException;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.*;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

public class TestOMRSArchiveBuilder
{
    @Test
    public void testAddEntityDef()
    {
        OMRSArchiveBuilder oMRSArchiveBuilder = getOmrsArchiveBuilder();
        EntityDef entityDefA = new EntityDef(TypeDefCategory.ENTITY_DEF,"1111","EntityDefA",1L,"1");
        oMRSArchiveBuilder.addEntityDef(entityDefA);

    }
    @Test
    public void testAddRelationshipDef()
    {
        OMRSArchiveBuilder oMRSArchiveBuilder = getOmrsArchiveBuilder();
        EntityDef entityDefA = new EntityDef(TypeDefCategory.ENTITY_DEF,"1111","EntityDefA",1L,"1");
        oMRSArchiveBuilder.addEntityDef(entityDefA);
        EntityDef entityDefB = new EntityDef(TypeDefCategory.ENTITY_DEF,"2222","EntityDefB",1L,"1");
        oMRSArchiveBuilder.addEntityDef(entityDefB);
        assertNotNull(oMRSArchiveBuilder);

        List<TypeDefAttribute> propertyList1 = new ArrayList();
        TypeDefAttribute attr1 = new TypeDefAttribute();
        attr1.setAttributeName("abc");
        propertyList1.add(attr1);
        entityDefA.setPropertiesDefinition(propertyList1);

        List<TypeDefAttribute> propertyList2 = new ArrayList();
        TypeDefAttribute attr2 = new TypeDefAttribute();
        attr2.setAttributeName("abc2");
        propertyList2.add(attr2);
        entityDefB.setPropertiesDefinition(propertyList2);

        RelationshipDef relationshipDef = new RelationshipDef(TypeDefCategory.RELATIONSHIP_DEF,"3333","EntityDef1",1L,"1");
        RelationshipEndDef endDef1 = new RelationshipEndDef();
        endDef1.setAttributeName("aaaa");

        TypeDefLink entityTypeA = new TypeDefLink();
        entityTypeA.setGUID("1111");
        entityTypeA.setName("EntityDefA");
        endDef1.setEntityType(entityTypeA);
        relationshipDef.setEndDef1(endDef1);

        RelationshipEndDef endDef2 = new RelationshipEndDef();
        endDef2.setAttributeName("bbbb");
        TypeDefLink entityTypeB= new TypeDefLink();
        entityTypeB.setGUID("2222");
        entityTypeB.setName("EntityDefB");
        endDef2.setEntityType(entityTypeB);
        relationshipDef.setEndDef2(endDef2);

        List<TypeDefAttribute> propertyList3 = new ArrayList();
        TypeDefAttribute attr3 = new TypeDefAttribute();
        attr3.setAttributeName("abc3");
        propertyList3.add(attr3);
        relationshipDef.setPropertiesDefinition(propertyList3);
        oMRSArchiveBuilder.addRelationshipDef(relationshipDef);
    }

    @Test
    public void testAddEntityDefSameAttr()
    {
        OMRSArchiveBuilder oMRSArchiveBuilder = getOmrsArchiveBuilder();
        EntityDef entityDefA = new EntityDef(TypeDefCategory.ENTITY_DEF,"1111","EntityDefA",1L,"1");
        List<TypeDefAttribute> propertyList1 = new ArrayList();
        TypeDefAttribute attr1 = new TypeDefAttribute();
        attr1.setAttributeName("abc");
        propertyList1.add(attr1);
        TypeDefAttribute attr2 = new TypeDefAttribute();
        attr2.setAttributeName("abc");
        propertyList1.add(attr2);
        entityDefA.setPropertiesDefinition(propertyList1);
        try {
            oMRSArchiveBuilder.addEntityDef(entityDefA);
            assertTrue(false);
        } catch (OMRSLogicErrorException e) {
            assertTrue(e.getMessage().contains("OMRS-ARCHIVEBUILDER-400-009"));
        }
    }
    @Test
    public void testAddClassificationDefSameAttr() {
        OMRSArchiveBuilder oMRSArchiveBuilder = getOmrsArchiveBuilder();
        ClassificationDef classificationDef = new ClassificationDef(TypeDefCategory.CLASSIFICATION_DEF,"1111","EntityDefA",1L,"1");
        List<TypeDefAttribute> propertyList1 = new ArrayList();
        TypeDefAttribute attr1 = new TypeDefAttribute();
        attr1.setAttributeName("abc");
        propertyList1.add(attr1);
        TypeDefAttribute attr2 = new TypeDefAttribute();
        attr2.setAttributeName("abc");
        propertyList1.add(attr2);
        classificationDef.setPropertiesDefinition(propertyList1);
        try {
            oMRSArchiveBuilder.addClassificationDef(classificationDef);
            assertTrue(false);
        } catch (OMRSLogicErrorException e) {
            assertTrue(e.getMessage().contains("OMRS-ARCHIVEBUILDER-400-010"));
        }
    }
    @Test
    public void testAddRelationshipDefSameAttr() {
        OMRSArchiveBuilder oMRSArchiveBuilder = getOmrsArchiveBuilder();

        EntityDef entityDefA = new EntityDef(TypeDefCategory.ENTITY_DEF,"1111","EntityDefA",1L,"1");
        List<TypeDefAttribute> propertyList1 = new ArrayList();
        TypeDefAttribute attr1 = new TypeDefAttribute();
        attr1.setAttributeName("abc");
        propertyList1.add(attr1);
        entityDefA.setPropertiesDefinition(propertyList1);
        oMRSArchiveBuilder.addEntityDef(entityDefA);

        EntityDef entityDefB = new EntityDef(TypeDefCategory.ENTITY_DEF,"2222","EntityDefB",1L,"1");
        List<TypeDefAttribute> propertyList2 = new ArrayList();
        TypeDefAttribute attr2 = new TypeDefAttribute();
        attr2.setAttributeName("abc2");
        propertyList2.add(attr2);
        entityDefB.setPropertiesDefinition(propertyList2);
        oMRSArchiveBuilder.addEntityDef(entityDefB);

        RelationshipDef relationshipDef = new RelationshipDef(TypeDefCategory.RELATIONSHIP_DEF,"3333","RelDef1",1L,"1");
        RelationshipEndDef endDef1 = new RelationshipEndDef();
        endDef1.setAttributeName("aaaa");

        TypeDefLink entityTypeA = new TypeDefLink();
        entityTypeA.setGUID("1111");
        entityTypeA.setName("EntityDefA");
        endDef1.setEntityType(entityTypeA);
        relationshipDef.setEndDef1(endDef1);

        RelationshipEndDef endDef2 = new RelationshipEndDef();
        endDef2.setAttributeName("bbbb");
        TypeDefLink entityTypeB= new TypeDefLink();
        entityTypeB.setGUID("2222");
        entityTypeB.setName("EntityDefB");
        endDef2.setEntityType(entityTypeB);
        relationshipDef.setEndDef2(endDef2);

        List<TypeDefAttribute> propertyList3 = new ArrayList();
        TypeDefAttribute attr3 = new TypeDefAttribute();
        attr3.setAttributeName("abc3");
        TypeDefAttribute attr4 = new TypeDefAttribute();
        attr4.setAttributeName("abc3");
        propertyList3.add(attr3);
        propertyList3.add(attr4);

        relationshipDef.setPropertiesDefinition(propertyList3);
        try {
            oMRSArchiveBuilder.addRelationshipDef(relationshipDef);
            assertTrue(false);
        } catch (OMRSLogicErrorException e) {
            assertTrue(e.getMessage().contains("OMRS-ARCHIVEBUILDER-400-008"));
        }
    }


    @Test
    public void testAddRelationshipDefDuplicateEnd2()
    {
        OMRSArchiveBuilder oMRSArchiveBuilder = getOmrsArchiveBuilder();
        EntityDef entityDefA = new EntityDef(TypeDefCategory.ENTITY_DEF,"1111","EntityDefA",1L,"1");
        oMRSArchiveBuilder.addEntityDef(entityDefA);
        EntityDef entityDefB = new EntityDef(TypeDefCategory.ENTITY_DEF,"2222","EntityDefB",1L,"1");
        oMRSArchiveBuilder.addEntityDef(entityDefB);
        EntityDef entityDefC = new EntityDef(TypeDefCategory.ENTITY_DEF,"1122","EntityDefC",1L,"1");
        oMRSArchiveBuilder.addEntityDef(entityDefC);

        RelationshipDef relationshipDef1 = new RelationshipDef(TypeDefCategory.RELATIONSHIP_DEF,"3333","RelDef1",1L,"1");
        createEndDefs(relationshipDef1,"1111","aaaa","EntityDefA","2222","bbbb","EntityDefB");
        oMRSArchiveBuilder.addRelationshipDef(relationshipDef1);

        RelationshipDef relationshipDef2 = new RelationshipDef(TypeDefCategory.RELATIONSHIP_DEF,"4444","RelDef2",1L,"1");
        createEndDefs(relationshipDef2,"1111","aaaa","EntityDefA","1122","bbbb","EntityDefC");

        try {
            oMRSArchiveBuilder.addRelationshipDef(relationshipDef2);
            assertTrue(false);
        } catch (OMRSLogicErrorException e) {
            assertTrue(e.getMessage().contains("OMRS-ARCHIVEBUILDER-400-007"));
        }
    }

    @Test
    public void testAddRelationshipDefDuplicateEnd1()
    {
        OMRSArchiveBuilder oMRSArchiveBuilder = getOmrsArchiveBuilder();
        EntityDef entityDefA = new EntityDef(TypeDefCategory.ENTITY_DEF,"1111","EntityDefA",1L,"1");
        oMRSArchiveBuilder.addEntityDef(entityDefA);
        EntityDef entityDefB = new EntityDef(TypeDefCategory.ENTITY_DEF,"2222","EntityDefB",1L,"1");
        oMRSArchiveBuilder.addEntityDef(entityDefB);
        EntityDef entityDefC = new EntityDef(TypeDefCategory.ENTITY_DEF,"1122","EntityDefC",1L,"1");
        oMRSArchiveBuilder.addEntityDef(entityDefC);

        List<TypeDefAttribute> propertyList1 = new ArrayList();
        TypeDefAttribute attr1 = new TypeDefAttribute();
        attr1.setAttributeName("abc");
        propertyList1.add(attr1);
        entityDefA.setPropertiesDefinition(propertyList1);

        List<TypeDefAttribute> propertyList2 = new ArrayList();
        TypeDefAttribute attr2 = new TypeDefAttribute();
        attr2.setAttributeName("abc2");
        propertyList2.add(attr2);
        entityDefB.setPropertiesDefinition(propertyList2);

        RelationshipDef relationshipDef1 = new RelationshipDef(TypeDefCategory.RELATIONSHIP_DEF,"3333","RelDef1",1L,"1");
        createEndDefs(relationshipDef1,"2222","bbbb","EntityDefB","1111","aaaa","EntityDefA");
        oMRSArchiveBuilder.addRelationshipDef(relationshipDef1);

        RelationshipDef relationshipDef2 = new RelationshipDef(TypeDefCategory.RELATIONSHIP_DEF,"4444","RelDef2",1L,"1");
        createEndDefs(relationshipDef2,"1122","bbbb","EntityDefC","1111","aaaa","EntityDefA");

        try
        {
            oMRSArchiveBuilder.addRelationshipDef(relationshipDef2);
            assertTrue(false);
        } catch (OMRSLogicErrorException e)
        {
            assertTrue(e.getMessage().contains("OMRS-ARCHIVEBUILDER-400-006"));
        }
    }

    @Test
    public void testAddRelationshipDefDuplicateEnd1NameAndLocal()
    {
        OMRSArchiveBuilder oMRSArchiveBuilder = getOmrsArchiveBuilder();
        EntityDef entityDefA = new EntityDef(TypeDefCategory.ENTITY_DEF,"1111","EntityDefA",1L,"1");
        List<TypeDefAttribute> propertyList0 = new ArrayList();
        TypeDefAttribute attr = new TypeDefAttribute();
        attr.setAttributeName("aaaa");
        propertyList0.add(attr);
        entityDefA.setPropertiesDefinition(propertyList0);
        oMRSArchiveBuilder.addEntityDef(entityDefA);

        EntityDef entityDefB = new EntityDef(TypeDefCategory.ENTITY_DEF,"2222","EntityDefB",1L,"1");
        oMRSArchiveBuilder.addEntityDef(entityDefB);

        RelationshipDef relationshipDef1 = new RelationshipDef(TypeDefCategory.RELATIONSHIP_DEF,"3333","RelDef1",1L,"1");
        createEndDefs(relationshipDef1,"2222","aaaa","EntityDefB","1111","bbbb","EntityDefA");

        try
        {
            oMRSArchiveBuilder.addRelationshipDef(relationshipDef1);
            assertTrue(false);
        } catch (OMRSLogicErrorException e)
        {
            assertTrue(e.getMessage().contains("OMRS-ARCHIVEBUILDER-400-006"));
        }
    }
    @Test
    public void testAddRelationshipDefDuplicateEnd2NameAndLocal()
    {
        OMRSArchiveBuilder oMRSArchiveBuilder = getOmrsArchiveBuilder();
        EntityDef entityDefA = new EntityDef(TypeDefCategory.ENTITY_DEF,"1111","EntityDefA",1L,"1");
        List<TypeDefAttribute> propertyList = new ArrayList();
       ;
        oMRSArchiveBuilder.addEntityDef(entityDefA);

        EntityDef entityDefB = new EntityDef(TypeDefCategory.ENTITY_DEF,"2222","EntityDefB",1L,"1");
        TypeDefAttribute attr = new TypeDefAttribute();
        attr.setAttributeName("bbbb");
        propertyList.add(attr);
        entityDefB.setPropertiesDefinition(propertyList);
        oMRSArchiveBuilder.addEntityDef(entityDefB);

        RelationshipDef relationshipDef1 = new RelationshipDef(TypeDefCategory.RELATIONSHIP_DEF,"3333","RelDef1",1L,"1");
        createEndDefs(relationshipDef1,"2222","aaaa","EntityDefB","1111","bbbb","EntityDefA");

        try
        {
            oMRSArchiveBuilder.addRelationshipDef(relationshipDef1);
            assertTrue(false);
        } catch (OMRSLogicErrorException e)
        {
            assertTrue(e.getMessage().contains("OMRS-ARCHIVEBUILDER-400-007"));
        }
    }

    private void createEndDefs(RelationshipDef relationshipDef,String guid1,String name1, String type1,String guid2,String name2, String type2)
    {
        RelationshipEndDef endDef1 = new RelationshipEndDef();
        endDef1.setAttributeName(name1);
        TypeDefLink entityType1 = new TypeDefLink();
        entityType1.setGUID(guid1);
        entityType1.setName(type1);
        endDef1.setEntityType(entityType1);
        relationshipDef.setEndDef1(endDef1);

        RelationshipEndDef endDef2 = new RelationshipEndDef();
        endDef2.setAttributeName(name2);
        TypeDefLink entityType2 = new TypeDefLink();
        entityType2.setGUID(guid2);
        entityType2.setName(type2);
        endDef2.setEntityType(entityType2);
        relationshipDef.setEndDef2(endDef2);
    }

    private OMRSArchiveBuilder getOmrsArchiveBuilder()
    {
        OMRSArchiveBuilder archive = new OMRSArchiveBuilder("guid",
                    "testArchiveName",
                    "test description",
                     OpenMetadataArchiveType.CONTENT_PACK,
                    "testOwner",
                    null,
                    null);
        return archive;
    }
}