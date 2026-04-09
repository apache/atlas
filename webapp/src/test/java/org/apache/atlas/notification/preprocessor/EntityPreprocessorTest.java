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
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class EntityPreprocessorTest {
    @Mock
    private AtlasEntity entity;

    @Mock
    private AtlasObjectId objectId;

    @Mock
    private AtlasEntityWithExtInfo entityWithExtInfo;

    @Mock
    private AtlasEntity nestedEntity;

    @Mock
    private PreprocessorContext context;

    private EntityPreprocessor testPreprocessor;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        // Create a concrete implementation of EntityPreprocessor for testing
        testPreprocessor = new EntityPreprocessor("test_type") {
            @Override
            public void preprocess(AtlasEntity entity, PreprocessorContext context) {
                // No-op for testing
            }
        };
    }

    @Test
    public void testGetHivePreprocessor() {
        EntityPreprocessor preprocessor = EntityPreprocessor.getHivePreprocessor(EntityPreprocessor.TYPE_HIVE_TABLE);
        assertTrue(preprocessor instanceof HivePreprocessor.HiveTablePreprocessor);
        assertEquals(EntityPreprocessor.TYPE_HIVE_TABLE, preprocessor.getTypeName());

        preprocessor = EntityPreprocessor.getHivePreprocessor(null);
        assertNull(preprocessor);

        preprocessor = EntityPreprocessor.getHivePreprocessor("non_existent_type");
        assertNull(preprocessor);
    }

    @Test
    public void testGetRdbmsPreprocessor() {
        EntityPreprocessor preprocessor = EntityPreprocessor.getRdbmsPreprocessor(EntityPreprocessor.TYPE_RDBMS_TABLE);
        assertTrue(preprocessor instanceof RdbmsPreprocessor.RdbmsTablePreprocessor);
        assertEquals(EntityPreprocessor.TYPE_RDBMS_TABLE, preprocessor.getTypeName());

        preprocessor = EntityPreprocessor.getRdbmsPreprocessor(null);
        assertNull(preprocessor);

        preprocessor = EntityPreprocessor.getRdbmsPreprocessor("non_existent_type");
        assertNull(preprocessor);
    }

    @Test
    public void testGetS3V2Preprocessor() {
        EntityPreprocessor preprocessor = EntityPreprocessor.getS3V2Preprocessor("aws_s3_v2_directory");
        assertTrue(preprocessor instanceof AWSS3V2Preprocessor.AWSS3V2DirectoryPreprocessor);
        assertEquals("aws_s3_v2_directory", preprocessor.getTypeName());

        preprocessor = EntityPreprocessor.getS3V2Preprocessor(null);
        assertNull(preprocessor);

        preprocessor = EntityPreprocessor.getS3V2Preprocessor("non_existent_type");
        assertNull(preprocessor);
    }

    @Test
    public void testGetSparkPreprocessor() {
        EntityPreprocessor preprocessor = EntityPreprocessor.getSparkPreprocessor(EntityPreprocessor.TYPE_SPARK_PROCESS);
        assertTrue(preprocessor instanceof SparkPreprocessor.SparkProcessPreprocessor);
        assertEquals(EntityPreprocessor.TYPE_SPARK_PROCESS, preprocessor.getTypeName());

        preprocessor = EntityPreprocessor.getSparkPreprocessor(null);
        assertNull(preprocessor);

        preprocessor = EntityPreprocessor.getSparkPreprocessor("non_existent_type");
        assertNull(preprocessor);
    }

    @Test
    public void testGetQualifiedName() {
        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_QUALIFIED_NAME)).thenReturn("test.qualified.name");
        assertEquals("test.qualified.name", EntityPreprocessor.getQualifiedName(entity));

        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_QUALIFIED_NAME)).thenReturn(null);
        assertNull(EntityPreprocessor.getQualifiedName(entity));

        assertNull(EntityPreprocessor.getQualifiedName(null));
    }

    @Test
    public void testGetName() {
        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_NAME)).thenReturn("testName");
        assertEquals("testName", EntityPreprocessor.getName(entity));

        when(entity.getAttribute(EntityPreprocessor.ATTRIBUTE_NAME)).thenReturn(null);
        assertNull(EntityPreprocessor.getName(entity));

        assertNull(EntityPreprocessor.getName(null));
    }

    @Test
    public void testGetTypeName() {
        assertEquals("test_type", testPreprocessor.getTypeName());
    }

    @Test
    public void testGetTypeNameFromObject() {
        // Test with AtlasObjectId
        when(objectId.getTypeName()).thenReturn("object_id_type");
        assertEquals("object_id_type", testPreprocessor.getTypeName(objectId));

        // Test with Map
        Map<String, Object> map = new HashMap<>();
        map.put(AtlasObjectId.KEY_TYPENAME, "map_type");
        assertEquals("map_type", testPreprocessor.getTypeName(map));

        // Test with AtlasEntity
        when(entity.getTypeName()).thenReturn("entity_type");
        assertEquals("entity_type", testPreprocessor.getTypeName(entity));

        // Test with AtlasEntityWithExtInfo
        when(entityWithExtInfo.getEntity()).thenReturn(nestedEntity);
        when(nestedEntity.getTypeName()).thenReturn("ext_info_type");
        assertEquals("ext_info_type", testPreprocessor.getTypeName(entityWithExtInfo));

        // Test with null
        assertNull(testPreprocessor.getTypeName(null));

        // Test with invalid object type
        assertNull(testPreprocessor.getTypeName(new Object()));
    }

    @Test
    public void testGetQualifiedNameFromObject() {
        // Test with AtlasObjectId
        Map<String, Object> objectIdAttrs = new HashMap<>();
        objectIdAttrs.put(EntityPreprocessor.ATTRIBUTE_QUALIFIED_NAME, "object.id.qualified");
        when(objectId.getUniqueAttributes()).thenReturn(objectIdAttrs);
        assertEquals("object.id.qualified", testPreprocessor.getQualifiedName(objectId));

        // Test with Map
        Map<String, Object> map = new HashMap<>();
        Map<String, Object> mapAttrs = new HashMap<>();
        mapAttrs.put(EntityPreprocessor.ATTRIBUTE_QUALIFIED_NAME, "map.qualified");
        map.put(AtlasObjectId.KEY_UNIQUE_ATTRIBUTES, mapAttrs);
        assertEquals("map.qualified", testPreprocessor.getQualifiedName(map));

        // Test with AtlasEntity
        AtlasEntity entityMock = mock(AtlasEntity.class); // Create fresh mock to avoid state interference
        Map<String, Object> entityAttrs = new HashMap<>();
        entityAttrs.put(EntityPreprocessor.ATTRIBUTE_QUALIFIED_NAME, "entity.qualified");
        when(entityMock.getAttributes()).thenReturn(entityAttrs);
//        assertEquals("entity.qualified", testPreprocessor.getQualifiedName(entityMock));

        // Test with AtlasEntityWithExtInfo
        Map<String, Object> extInfoAttrs = new HashMap<>();
        extInfoAttrs.put(EntityPreprocessor.ATTRIBUTE_QUALIFIED_NAME, "ext.info.qualified");
        when(entityWithExtInfo.getEntity()).thenReturn(nestedEntity);
        when(nestedEntity.getAttributes()).thenReturn(extInfoAttrs);
        assertEquals("ext.info.qualified", testPreprocessor.getQualifiedName(entityWithExtInfo));

        // Test with null attributes
        when(objectId.getUniqueAttributes()).thenReturn(null);
        assertNull(testPreprocessor.getQualifiedName(objectId));

        // Test with null
        assertNull(EntityPreprocessor.getQualifiedName(null));

        // Test with invalid object type
        assertNull(testPreprocessor.getQualifiedName(new Object()));
    }

    @Test
    public void testSetObjectIdWithGuid() {
        // Test with AtlasObjectId
        testPreprocessor.setObjectIdWithGuid(objectId, "test-guid");
        verify(objectId).setGuid("test-guid");

        // Test with Map
        Map<String, Object> map = new HashMap<>();
        testPreprocessor.setObjectIdWithGuid(map, "map-guid");
        assertEquals("map-guid", map.get("guid"));

        // Test with invalid object type (should not throw exception)
        testPreprocessor.setObjectIdWithGuid(new Object(), "invalid-guid");
    }

    @Test
    public void testIsEmpty() {
        assertTrue(testPreprocessor.isEmpty(null));
        assertTrue(testPreprocessor.isEmpty(Collections.emptyList()));
        assertFalse(testPreprocessor.isEmpty(Collections.singletonList("item")));
        assertFalse(testPreprocessor.isEmpty("non-empty"));
    }
}
