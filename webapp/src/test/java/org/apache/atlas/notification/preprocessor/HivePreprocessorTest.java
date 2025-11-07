/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.atlas.notification.preprocessor;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.notification.preprocessor.PreprocessorContext.PreprocessAction;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class HivePreprocessorTest {
    @Mock
    private AtlasEntity entity;

    @Mock
    private PreprocessorContext context;

    @Mock
    private AtlasObjectId objectId1;

    @Mock
    private AtlasObjectId objectId2;

    @Mock
    private AtlasEntity referredEntity;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testHiveDbPreprocessorConstructor() {
        HivePreprocessor.HiveDbPreprocessor preprocessor = new HivePreprocessor.HiveDbPreprocessor();
        assertEquals(EntityPreprocessor.TYPE_HIVE_DB, preprocessor.getTypeName());
    }

    @Test
    public void testHiveDbPreprocessorPreprocessIgnore() {
        HivePreprocessor.HiveDbPreprocessor preprocessor = new HivePreprocessor.HiveDbPreprocessor();
        when(entity.getGuid()).thenReturn("guid1");
        when(context.isIgnoredEntity("guid1")).thenReturn(false);
        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_NAME)).thenReturn("testDb");
        when(context.getPreprocessActionForHiveDb("testDb")).thenReturn(PreprocessAction.IGNORE);

        preprocessor.preprocess(entity, context);

        verify(context).addToIgnoredEntities(entity);
    }

    @Test
    public void testHiveDbPreprocessorPreprocessNotIgnored() {
        HivePreprocessor.HiveDbPreprocessor preprocessor = new HivePreprocessor.HiveDbPreprocessor();
        when(entity.getGuid()).thenReturn("guid1");
        when(context.isIgnoredEntity("guid1")).thenReturn(false);
        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_NAME)).thenReturn("testDb");

        preprocessor.preprocess(entity, context);

        verify(context, never()).addToIgnoredEntities(entity);
    }

    @Test
    public void testHiveTablePreprocessorConstructor() {
        HivePreprocessor.HiveTablePreprocessor preprocessor = new HivePreprocessor.HiveTablePreprocessor();
        assertEquals(EntityPreprocessor.TYPE_HIVE_TABLE, preprocessor.getTypeName());
    }

    @Test
    public void testHiveTablePreprocessorPreprocessIgnore() {
        HivePreprocessor.HiveTablePreprocessor preprocessor = new HivePreprocessor.HiveTablePreprocessor();
        when(entity.getGuid()).thenReturn("guid1");
        when(context.isIgnoredEntity("guid1")).thenReturn(false);
        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_QUALIFIED_NAME)).thenReturn("db.table@cluster");
        when(context.getPreprocessActionForHiveTable("db.table@cluster")).thenReturn(PreprocessAction.IGNORE);
        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_SD)).thenReturn(objectId1);
        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_COLUMNS)).thenReturn(Arrays.asList(objectId1, objectId2));
        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_PARTITION_KEYS)).thenReturn(Collections.emptyList());
        when(entity.getAttribute(EntityPreprocessor.TYPE_HIVE_TABLE_DDL)).thenReturn(objectId2);

        preprocessor.preprocess(entity, context);

        verify(context).addToIgnoredEntities(entity);
        verify(context).addToIgnoredEntities(objectId1);
        verify(context).addToIgnoredEntities(Arrays.asList(objectId1, objectId2));
        verify(context).addToIgnoredEntities(Collections.emptyList());
        verify(context).addToIgnoredEntities(objectId2);
    }

    @Test
    public void testHiveTablePreprocessorPreprocessPrune() {
        HivePreprocessor.HiveTablePreprocessor preprocessor = new HivePreprocessor.HiveTablePreprocessor();
        when(entity.getGuid()).thenReturn("guid1");
        when(context.isIgnoredEntity("guid1")).thenReturn(false);
        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_QUALIFIED_NAME)).thenReturn("db.table@cluster");
        when(context.getPreprocessActionForHiveTable("db.table@cluster")).thenReturn(PreprocessAction.PRUNE);
        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_SD)).thenReturn(objectId1);
        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_COLUMNS)).thenReturn(Arrays.asList(objectId1, objectId2));
        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_PARTITION_KEYS)).thenReturn(Collections.emptyList());

        preprocessor.preprocess(entity, context);

        verify(context).addToPrunedEntities(entity);
        verify(context).addToIgnoredEntities(objectId1);
        verify(context).addToIgnoredEntities(Arrays.asList(objectId1, objectId2));
        verify(context).addToIgnoredEntities(Collections.emptyList());
        verify(entity).setAttribute(EntityPreprocessor.ATTRIBUTE_SD, null);
        verify(entity).setAttribute(EntityPreprocessor.ATTRIBUTE_COLUMNS, null);
        verify(entity).setAttribute(EntityPreprocessor.ATTRIBUTE_PARTITION_KEYS, null);
        verify(entity).setRelationshipAttribute(EntityPreprocessor.ATTRIBUTE_SD, null);
        verify(entity).setRelationshipAttribute(EntityPreprocessor.ATTRIBUTE_COLUMNS, null);
        verify(entity).setRelationshipAttribute(EntityPreprocessor.ATTRIBUTE_PARTITION_KEYS, null);
    }

    @Test
    public void testHiveTablePreprocessorPreprocessRemoveOwnedRefAttrs() {
        HivePreprocessor.HiveTablePreprocessor preprocessor = new HivePreprocessor.HiveTablePreprocessor();
        when(entity.getGuid()).thenReturn("guid1");
        when(context.isIgnoredEntity("guid1")).thenReturn(false);
        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_QUALIFIED_NAME)).thenReturn("db.table@cluster");
        when(context.getHiveTypesRemoveOwnedRefAttrs()).thenReturn(true);

        preprocessor.preprocess(entity, context);

        verify(context).removeRefAttributeAndRegisterToMove(entity, EntityPreprocessor.ATTRIBUTE_SD, "hive_table_storagedesc", EntityPreprocessor.ATTRIBUTE_TABLE);
        verify(context).removeRefAttributeAndRegisterToMove(entity, EntityPreprocessor.ATTRIBUTE_COLUMNS, "hive_table_columns", EntityPreprocessor.ATTRIBUTE_TABLE);
        verify(context).removeRefAttributeAndRegisterToMove(entity, EntityPreprocessor.ATTRIBUTE_PARTITION_KEYS, "hive_table_partitionkeys", EntityPreprocessor.ATTRIBUTE_TABLE);
    }

    @Test
    public void testHiveColumnPreprocessorConstructor() {
        HivePreprocessor.HiveColumnPreprocessor preprocessor = new HivePreprocessor.HiveColumnPreprocessor();
        assertEquals(EntityPreprocessor.TYPE_HIVE_COLUMN, preprocessor.getTypeName());
    }

    @Test
    public void testHiveColumnPreprocessorGetHiveTableQualifiedName() {
        String qualifiedName = "db.table.column@cluster";
        String result = HivePreprocessor.HiveColumnPreprocessor.getHiveTableQualifiedName(qualifiedName);
        assertEquals("db.table@cluster", result);

        qualifiedName = "db.table.column";
        result = HivePreprocessor.HiveColumnPreprocessor.getHiveTableQualifiedName(qualifiedName);
        assertEquals("db.table", result);

        qualifiedName = "db.table@cluster";
        result = HivePreprocessor.HiveColumnPreprocessor.getHiveTableQualifiedName(qualifiedName);
        assertEquals("db@cluster", result);
    }

    @Test
    public void testHiveColumnPreprocessorPreprocessIgnore() {
        HivePreprocessor.HiveColumnPreprocessor preprocessor = new HivePreprocessor.HiveColumnPreprocessor();
        when(entity.getGuid()).thenReturn("guid1");
        when(context.isIgnoredEntity("guid1")).thenReturn(false);
        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_QUALIFIED_NAME)).thenReturn("db.table.column@cluster");
        when(context.getPreprocessActionForHiveTable("db.table@cluster")).thenReturn(PreprocessAction.IGNORE);

        preprocessor.preprocess(entity, context);

        verify(context).addToIgnoredEntities("guid1");
    }

    @Test
    public void testHiveColumnPreprocessorPreprocessNotIgnored() {
        HivePreprocessor.HiveColumnPreprocessor preprocessor = new HivePreprocessor.HiveColumnPreprocessor();
        when(entity.getGuid()).thenReturn("guid1");
        when(context.isIgnoredEntity("guid1")).thenReturn(false);
        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_QUALIFIED_NAME)).thenReturn("db.table.column@cluster");

        preprocessor.preprocess(entity, context);

        verify(context, never()).addToIgnoredEntities(anyString());
    }

    @Test
    public void testHiveStorageDescPreprocessorConstructor() {
        HivePreprocessor.HiveStorageDescPreprocessor preprocessor = new HivePreprocessor.HiveStorageDescPreprocessor();
        assertEquals(EntityPreprocessor.TYPE_HIVE_STORAGEDESC, preprocessor.getTypeName());
    }

    @Test
    public void testHiveStorageDescPreprocessorGetHiveTableQualifiedName() {
        String qualifiedName = "db.table_storage@cluster";
        String result = HivePreprocessor.HiveStorageDescPreprocessor.getHiveTableQualifiedName(qualifiedName);
        assertEquals("db.table", result);

        qualifiedName = "db.table";
        result = HivePreprocessor.HiveStorageDescPreprocessor.getHiveTableQualifiedName(qualifiedName);
        assertEquals("db.table", result);
    }

    @Test
    public void testHiveStorageDescPreprocessorPreprocessNotIgnored() {
        HivePreprocessor.HiveStorageDescPreprocessor preprocessor = new HivePreprocessor.HiveStorageDescPreprocessor();
        when(entity.getGuid()).thenReturn("guid1");
        when(context.isIgnoredEntity("guid1")).thenReturn(false);
        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_QUALIFIED_NAME)).thenReturn("db.table_storage@cluster");

        preprocessor.preprocess(entity, context);

        verify(context, never()).addToIgnoredEntities(anyString());
    }

    @Test
    public void testHiveProcessPreprocessorConstructor() {
        HivePreprocessor.HiveProcessPreprocessor preprocessor = new HivePreprocessor.HiveProcessPreprocessor();
        assertEquals(EntityPreprocessor.TYPE_HIVE_PROCESS, preprocessor.getTypeName());

        preprocessor = new HivePreprocessor.HiveProcessPreprocessor("custom_type");
        assertEquals("custom_type", preprocessor.getTypeName());
    }

    @Test
    public void testHiveProcessPreprocessorPreprocessUpdateName() {
        HivePreprocessor.HiveProcessPreprocessor preprocessor = new HivePreprocessor.HiveProcessPreprocessor();
        when(context.updateHiveProcessNameWithQualifiedName()).thenReturn(true);
        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_NAME)).thenReturn("oldName");
        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_QUALIFIED_NAME)).thenReturn("newName");
        when(entity.getGuid()).thenReturn("guid1");
        when(context.isIgnoredEntity("guid1")).thenReturn(false);

        preprocessor.preprocess(entity, context);

        verify(entity).setAttribute(EntityPreprocessor.ATTRIBUTE_NAME, "newName");
    }

    @Test
    public void testHiveProcessPreprocessorPreprocessSetTimestamps() {
        HivePreprocessor.HiveProcessPreprocessor preprocessor = new HivePreprocessor.HiveProcessPreprocessor();
        when(context.updateHiveProcessNameWithQualifiedName()).thenReturn(false);
        when(entity.getGuid()).thenReturn("guid1");
        when(context.isIgnoredEntity("guid1")).thenReturn(false);
        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_START_TIME)).thenReturn(null);
        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_END_TIME)).thenReturn("null");

        preprocessor.preprocess(entity, context);

        verify(entity).setAttribute(eq(EntityPreprocessor.ATTRIBUTE_START_TIME), anyLong());
        verify(entity).setAttribute(eq(EntityPreprocessor.ATTRIBUTE_END_TIME), anyLong());
    }

    @Test
    public void testHiveProcessPreprocessorPreprocessIgnoreDueToEmptyInputsOutputs() {
        HivePreprocessor.HiveProcessPreprocessor preprocessor = new HivePreprocessor.HiveProcessPreprocessor();
        when(context.updateHiveProcessNameWithQualifiedName()).thenReturn(false);
        when(entity.getGuid()).thenReturn("guid1");
        when(context.isIgnoredEntity("guid1")).thenReturn(false);
        List<AtlasObjectId> inputs = new ArrayList<>(Collections.singletonList(objectId1));
        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_INPUTS)).thenReturn(inputs);
        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_OUTPUTS)).thenReturn(new ArrayList<>());
        when(context.getGuid(objectId1)).thenReturn("guid2");
        when(context.isIgnoredEntity("guid2")).thenReturn(true);

        preprocessor.preprocess(entity, context);

        verify(context).addToIgnoredEntities(entity);
    }

    @Test
    public void testHiveProcessPreprocessorPreprocessCheckpoint() {
        HivePreprocessor.HiveProcessPreprocessor preprocessor = new HivePreprocessor.HiveProcessPreprocessor();
        when(context.isSpooledMessage()).thenReturn(true);
        when(entity.getGuid()).thenReturn("guid1");
        when(context.isIgnoredEntity("guid1")).thenReturn(false);
        when(entity.getRelationshipAttribute(EntityPreprocessor.ATTRIBUTE_INPUTS)).thenReturn(Collections.singletonList(objectId1));
        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_QUALIFIED_NAME)).thenReturn("process1");
        when(objectId1.getUniqueAttributes()).thenReturn(Collections.singletonMap(EntityPreprocessor.ATTRIBUTE_QUALIFIED_NAME, "input_process1"));
        when(context.getGuidForDeletedEntity("input_process1")).thenReturn("guid2");

        preprocessor.preprocess(entity, context);

        verify(objectId1).setGuid("guid2");
    }

    @Test
    public void testHiveProcessPreprocessorRemoveIgnoredObjectIds() throws Exception {
        HivePreprocessor.HiveProcessPreprocessor preprocessor = new HivePreprocessor.HiveProcessPreprocessor();
        List<AtlasObjectId> inputs = new ArrayList<>(Arrays.asList(objectId1, objectId2));
        when(context.getGuid(objectId1)).thenReturn("guid1");
        when(context.getGuid(objectId2)).thenReturn("guid2");
        when(context.isIgnoredEntity("guid1")).thenReturn(true);
        when(context.isIgnoredEntity("guid2")).thenReturn(false);
        when(objectId2.getTypeName()).thenReturn(EntityPreprocessor.TYPE_HIVE_TABLE);
        when(objectId2.getUniqueAttributes()).thenReturn(Collections.singletonMap(EntityPreprocessor.ATTRIBUTE_QUALIFIED_NAME, "db.table@cluster"));

        // Use reflection to invoke private method
        Method removeIgnoredObjectIdsMethod = HivePreprocessor.HiveProcessPreprocessor.class
                .getDeclaredMethod("removeIgnoredObjectIds", Object.class, PreprocessorContext.class);
        removeIgnoredObjectIdsMethod.setAccessible(true);
        removeIgnoredObjectIdsMethod.invoke(preprocessor, inputs, context);

        assertEquals(1, inputs.size());
        assertEquals(objectId2, inputs.get(0));
    }

    @Test
    public void testHiveColumnLineageProcessPreprocessorConstructor() {
        HivePreprocessor.HiveColumnLineageProcessPreprocessor preprocessor = new HivePreprocessor.HiveColumnLineageProcessPreprocessor();
        assertEquals(EntityPreprocessor.TYPE_HIVE_COLUMN_LINEAGE, preprocessor.getTypeName());
    }
}
