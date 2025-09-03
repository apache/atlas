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
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class HiveTableDDLPreprocessorTest {
    @Mock
    private AtlasEntity entity;

    @Mock
    private PreprocessorContext context;

    @Mock
    private AtlasObjectId tableObject;

    private HiveTableDDLPreprocessor preprocessor;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        preprocessor = new HiveTableDDLPreprocessor();
    }

    @Test
    public void testConstructor() {
        assertEquals(EntityPreprocessor.TYPE_HIVE_TABLE_DDL, preprocessor.getTypeName());
    }

    @Test
    public void testPreprocessNotSpooledMessage() {
        when(context.isSpooledMessage()).thenReturn(false);

        preprocessor.preprocess(entity, context);

        verify(context).isSpooledMessage();
        verifyNoMoreInteractions(context, entity);
    }

    @Test
    public void testPreprocessNullTableObject() {
        when(context.isSpooledMessage()).thenReturn(true);
        when(entity.getRelationshipAttribute(EntityPreprocessor.ATTRIBUTE_TABLE)).thenReturn(null);

        preprocessor.preprocess(entity, context);

        verify(context).isSpooledMessage();
        verify(entity).getRelationshipAttribute(EntityPreprocessor.ATTRIBUTE_TABLE);
        verifyNoMoreInteractions(context, entity);
    }

    @Test
    public void testPreprocessEmptyGuid() {
        when(context.isSpooledMessage()).thenReturn(true);
        when(entity.getRelationshipAttribute(EntityPreprocessor.ATTRIBUTE_TABLE)).thenReturn(tableObject);
        when(tableObject.getUniqueAttributes()).thenReturn(Collections.singletonMap(EntityPreprocessor.ATTRIBUTE_QUALIFIED_NAME, "db.table@cluster"));
        when(context.getGuidForDeletedEntity("db.table@cluster")).thenReturn("");

        preprocessor.preprocess(entity, context);

        verify(context).isSpooledMessage();
        verify(entity).getRelationshipAttribute(EntityPreprocessor.ATTRIBUTE_TABLE);
        verify(context).getGuidForDeletedEntity("db.table@cluster");
    }

    @Test
    public void testPreprocessValidGuid() throws Exception {
        when(context.isSpooledMessage()).thenReturn(true);
        when(entity.getRelationshipAttribute(EntityPreprocessor.ATTRIBUTE_TABLE)).thenReturn(tableObject);
        when(tableObject.getUniqueAttributes()).thenReturn(Collections.singletonMap(EntityPreprocessor.ATTRIBUTE_QUALIFIED_NAME, "db.table@cluster"));
        when(context.getGuidForDeletedEntity("db.table@cluster")).thenReturn("guid1");

        // Mock logger to verify debug message
        Logger mockLogger = mock(Logger.class);
        when(mockLogger.isDebugEnabled()).thenReturn(true);
        Field loggerField = HiveTableDDLPreprocessor.class.getDeclaredField("LOG");
        loggerField.setAccessible(true);

        preprocessor.preprocess(entity, context);

        verify(context).isSpooledMessage();
        verify(entity).getRelationshipAttribute(EntityPreprocessor.ATTRIBUTE_TABLE);
        verify(context).getGuidForDeletedEntity("db.table@cluster");
        verify(tableObject).setGuid("guid1");
    }
}
