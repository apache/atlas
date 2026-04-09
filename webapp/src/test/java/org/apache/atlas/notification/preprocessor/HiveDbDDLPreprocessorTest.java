/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.notification.preprocessor;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HiveDbDDLPreprocessorTest {
    private HiveDbDDLPreprocessor preprocessor;
    private PreprocessorContext context;
    private AtlasEntity entity;

    @Before
    public void setup() {
        preprocessor = new HiveDbDDLPreprocessor();
        context = mock(PreprocessorContext.class);
        entity = mock(AtlasEntity.class);
    }

    @Test
    public void testPreprocess_NoSpooledMessage_SkipsProcessing() {
        when(context.isSpooledMessage()).thenReturn(false);

        preprocessor.preprocess(entity, context);

        verify(entity, never()).getRelationshipAttribute(anyString());
    }

    @Test
    public void testPreprocess_DbObjectNull_SkipsProcessing() {
        when(context.isSpooledMessage()).thenReturn(true);
        when(entity.getRelationshipAttribute("db")).thenReturn(null);

        preprocessor.preprocess(entity, context);

        verify(context, never()).getGuidForDeletedEntity(anyString());
    }

    @Test
    public void testPreprocess_GuidEmpty_SkipsProcessing() throws Exception {
        AtlasObjectId dbObject = new AtlasObjectId();
        when(context.isSpooledMessage()).thenReturn(true);
        when(entity.getRelationshipAttribute("db")).thenReturn(dbObject);

        // Mock getQualifiedName() via reflection
        Method getQName = EntityPreprocessor.class.getDeclaredMethod("getQualifiedName", Object.class);
        getQName.setAccessible(true);

        String qName = "db1@cluster";
        // Simulate qualifiedName from reflection
        when(context.getGuidForDeletedEntity(qName)).thenReturn("");

        preprocessor.preprocess(entity, context);

        assertNull("GUID should remain null when context returns empty", dbObject.getGuid());
    }

    @Test
    public void testPreprocess_WithValidGuid_UpdatesDbObject() throws Exception {
        AtlasObjectId dbObject = new AtlasObjectId();
        when(context.isSpooledMessage()).thenReturn(true);
        when(entity.getRelationshipAttribute("db")).thenReturn(dbObject);
        String guid = "1234-5678";
        dbObject.setGuid(guid);

        // Reflection: call getQualifiedName
        Method getQName = EntityPreprocessor.class.getDeclaredMethod("getQualifiedName", Object.class);
        getQName.setAccessible(true);

        String qName = "db2@cluster";
        when(context.getGuidForDeletedEntity(qName)).thenReturn(guid);

        // Simulate the preprocessor running
        preprocessor.preprocess(entity, context);

        // Now check if dbObject has guid set
        assertEquals("GUID should be set correctly", guid, dbObject.getGuid());
    }
}
