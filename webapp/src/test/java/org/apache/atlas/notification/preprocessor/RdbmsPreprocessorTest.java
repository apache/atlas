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
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RdbmsPreprocessorTest {
    @Mock
    private AtlasEntity entity;

    @Mock
    private PreprocessorContext context;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.openMocks(this);
    }

    // ---------- Tests for RdbmsTypePreprocessor (and Instance/Db) ----------
    @Test
    public void testRdbmsInstancePreprocessor_RemovesDatabases() {
        RdbmsPreprocessor.RdbmsInstancePreprocessor pre = new RdbmsPreprocessor.RdbmsInstancePreprocessor();

        when(context.getRdbmsTypesRemoveOwnedRefAttrs()).thenReturn(true);
        when(entity.getTypeName()).thenReturn(EntityPreprocessor.TYPE_RDBMS_INSTANCE);

        pre.preprocess(entity, context);

        verify(context).removeRefAttributeAndRegisterToMove(
                entity,
                EntityPreprocessor.ATTRIBUTE_DATABASES,
                "rdbms_instance_databases",
                EntityPreprocessor.ATTRIBUTE_INSTANCE);
    }

    @Test
    public void testRdbmsDbPreprocessor_RemovesTables() {
        RdbmsPreprocessor.RdbmsDbPreprocessor pre = new RdbmsPreprocessor.RdbmsDbPreprocessor();

        when(context.getRdbmsTypesRemoveOwnedRefAttrs()).thenReturn(true);
        when(entity.getTypeName()).thenReturn(EntityPreprocessor.TYPE_RDBMS_DB);

        pre.preprocess(entity, context);

        verify(context).removeRefAttributeAndRegisterToMove(
                entity,
                EntityPreprocessor.ATTRIBUTE_TABLES,
                "rdbms_db_tables",
                EntityPreprocessor.ATTRIBUTE_DB);
    }

    @Test
    public void testRdbmsTablePreprocessor_RemovesColumnsIndexesFks() {
        RdbmsPreprocessor.RdbmsTablePreprocessor pre = new RdbmsPreprocessor.RdbmsTablePreprocessor();

        when(context.getRdbmsTypesRemoveOwnedRefAttrs()).thenReturn(true);
        when(entity.getTypeName()).thenReturn(EntityPreprocessor.TYPE_RDBMS_TABLE);

        pre.preprocess(entity, context);

        verify(context).removeRefAttributeAndRegisterToMove(
                entity,
                EntityPreprocessor.ATTRIBUTE_COLUMNS,
                "rdbms_table_columns",
                EntityPreprocessor.ATTRIBUTE_TABLE);
        verify(context).removeRefAttributeAndRegisterToMove(
                entity,
                EntityPreprocessor.ATTRIBUTE_INDEXES,
                "rdbms_table_indexes",
                EntityPreprocessor.ATTRIBUTE_TABLE);
        verify(context).removeRefAttributeAndRegisterToMove(
                entity,
                EntityPreprocessor.ATTRIBUTE_FOREIGN_KEYS,
                "rdbms_table_foreign_key",
                EntityPreprocessor.ATTRIBUTE_TABLE);
    }

    @Test
    public void testRdbmsTypePreprocessor_AlsoProcessesReferredEntities() {
        RdbmsPreprocessor.RdbmsDbPreprocessor pre = new RdbmsPreprocessor.RdbmsDbPreprocessor();

        AtlasEntity referredEntity = mock(AtlasEntity.class);
        when(referredEntity.getTypeName()).thenReturn(EntityPreprocessor.TYPE_RDBMS_TABLE);

        Map<String, AtlasEntity> referredEntities = new HashMap<>();
        referredEntities.put("id1", referredEntity);

        when(context.getRdbmsTypesRemoveOwnedRefAttrs()).thenReturn(true);
        when(context.getReferredEntities()).thenReturn(referredEntities);
        when(entity.getTypeName()).thenReturn(EntityPreprocessor.TYPE_RDBMS_DB);

        pre.preprocess(entity, context);

        verify(context).removeRefAttributeAndRegisterToMove(
                referredEntity,
                EntityPreprocessor.ATTRIBUTE_COLUMNS,
                "rdbms_table_columns",
                EntityPreprocessor.ATTRIBUTE_TABLE);
    }

    // ---------- Tests for RdbmsTablePreprocessor extra logic ----------
    @Test
    public void testRdbmsTablePreprocessor_DbAlreadyPresent_NoFixNeeded() {
        RdbmsPreprocessor.RdbmsTablePreprocessor pre = spy(new RdbmsPreprocessor.RdbmsTablePreprocessor());

        when(context.getRdbmsTypesRemoveOwnedRefAttrs()).thenReturn(false);
        when(entity.getRelationshipAttribute(EntityPreprocessor.ATTRIBUTE_DB)).thenReturn(new Object());

        pre.preprocess(entity, context);

        verify(entity, never()).setRelationshipAttribute(eq(EntityPreprocessor.ATTRIBUTE_DB), any());
    }

    @Test
    public void testRdbmsTablePreprocessor_DbInAttributes_NoFixNeeded() {
        RdbmsPreprocessor.RdbmsTablePreprocessor pre = spy(new RdbmsPreprocessor.RdbmsTablePreprocessor());

        when(context.getRdbmsTypesRemoveOwnedRefAttrs()).thenReturn(false);
        when(entity.getRelationshipAttribute(EntityPreprocessor.ATTRIBUTE_DB)).thenReturn(null);
        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_DB)).thenReturn(new Object());

        pre.preprocess(entity, context);

        verify(entity, never()).setRelationshipAttribute(eq(EntityPreprocessor.ATTRIBUTE_DB), any());
    }

    @Test
    public void testRdbmsTablePreprocessor_FixDbRelationship_Success() {
        RdbmsPreprocessor.RdbmsTablePreprocessor pre = spy(new RdbmsPreprocessor.RdbmsTablePreprocessor());

        when(context.getRdbmsTypesRemoveOwnedRefAttrs()).thenReturn(false);
        when(entity.getRelationshipAttribute(EntityPreprocessor.ATTRIBUTE_DB)).thenReturn(null);
        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_DB)).thenReturn(null);

        // simulate dbQualifiedName computed
        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_QUALIFIED_NAME))
                .thenReturn("db1.table1@cluster1");
        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_NAME))
                .thenReturn("table1");

        pre.preprocess(entity, context);

        AtlasObjectId expectedDbId = new AtlasObjectId(
                EntityPreprocessor.TYPE_RDBMS_DB,
                Collections.singletonMap(EntityPreprocessor.ATTRIBUTE_QUALIFIED_NAME, "db1@cluster1"));

        verify(entity).setRelationshipAttribute(eq(EntityPreprocessor.ATTRIBUTE_DB), eq(expectedDbId));
    }

    @Test
    public void testRdbmsTablePreprocessor_FixDbRelationship_NoQualifiedName() {
        RdbmsPreprocessor.RdbmsTablePreprocessor pre = spy(new RdbmsPreprocessor.RdbmsTablePreprocessor());

        when(context.getRdbmsTypesRemoveOwnedRefAttrs()).thenReturn(false);
        when(entity.getRelationshipAttribute(EntityPreprocessor.ATTRIBUTE_DB)).thenReturn(null);
        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_DB)).thenReturn(null);

        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_QUALIFIED_NAME)).thenReturn(null);
        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_NAME)).thenReturn("table1");

        pre.preprocess(entity, context);

        verify(entity, never()).setRelationshipAttribute(eq(EntityPreprocessor.ATTRIBUTE_DB), any());
    }
}
