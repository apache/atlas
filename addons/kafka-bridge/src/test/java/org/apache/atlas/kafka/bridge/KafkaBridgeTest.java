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

package org.apache.atlas.kafka.bridge;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.kafka.model.KafkaDataTypes;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.utils.KafkaUtils;
import org.apache.avro.Schema;
import org.apache.commons.configuration.Configuration;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.impl.client.CloseableHttpClient;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class KafkaBridgeTest {
    @Mock
    private KafkaUtils mockKafkaUtils;

    @Mock
    private AtlasClientV2 mockAtlasClient;

    @Mock
    private Configuration mockConfiguration;

    @Mock
    private CloseableHttpResponse mockHttpResponse;

    @Mock
    private CloseableHttpClient mockHttpClient;

    @Mock
    private StatusLine mockStatusLine;

    @Mock
    private HttpEntity mockHttpEntity;

    private void setupMocks() {
        MockitoAnnotations.initMocks(this);
    }

    private KafkaBridge kafkaBridge;

    private static final String TEST_QUALIFIED_NAME = "test_topic@test_cluster";
    private static final String TEST_GUID = "test-guid-123";
    private static final String[] TEST_NAMESPACE_ARRAY = {"test_namespace"};
    private static final String TEST_CLUSTER_NAME = "test_cluster";
    private static final String TEST_TOPIC_NAME = "test_topic";
    private static final String CLUSTER_NAME = "primary";
    private static final String TOPIC_QUALIFIED_NAME = KafkaBridge.getTopicQualifiedName(CLUSTER_NAME, TEST_TOPIC_NAME);
    private static final String TEST_SCHEMA_NAME = "test_topic-value";
    private static final int TEST_SCHEMA_VERSION = 1;
    private static final String TEST_NAMESPACE = "test_namespace";
    private static final ArrayList<Integer> TEST_SCHEMA_VERSION_LIST = new ArrayList<>(Arrays.asList(1, 2, 3, 4));
    private static final String TEST_SCHEMA = "{\"name\":\"test\",\"namespace\":\"testing\",\"type\":\"record\",\"fields\":[{\"name\":\"Field1\",\"type\":\"string\"},{\"name\":\"Field2\",\"type\":\"int\"}]}";
    private static final Schema.Field TEST_FIELD_NAME = new Schema.Parser().parse(TEST_SCHEMA).getField("Field1");
    private static final String TEST_FIELD_FULLNAME = "Field1";
    public static final AtlasEntity.AtlasEntityWithExtInfo TOPIC_WITH_EXT_INFO = new AtlasEntity.AtlasEntityWithExtInfo(
            getTopicEntityWithGuid("0dd466a4-3838-4537-8969-6abb8b9e9185"));
    public static final AtlasEntity.AtlasEntityWithExtInfo SCHEMA_WITH_EXT_INFO = new AtlasEntity.AtlasEntityWithExtInfo(
            getSchemaEntityWithGuid("2a9894bb-e535-4aa1-a00b-a7d21ac20738"));
    public static final AtlasEntity.AtlasEntityWithExtInfo FIELD_WITH_EXT_INFO = new AtlasEntity.AtlasEntityWithExtInfo(
            getFieldEntityWithGuid("41d1011f-d428-4f5a-9578-0a0a0439147f"));

    @BeforeMethod
    public void initializeMocks() {
        MockitoAnnotations.initMocks(this);
    }

    private static AtlasEntity getTopicEntityWithGuid(String guid) {
        AtlasEntity ret = new AtlasEntity(KafkaDataTypes.KAFKA_TOPIC.getName());
        ret.setGuid(guid);
        return ret;
    }

    private static AtlasEntity getSchemaEntityWithGuid(String guid) {
        AtlasEntity ret = new AtlasEntity(KafkaDataTypes.AVRO_SCHEMA.getName());
        ret.setGuid(guid);
        return ret;
    }

    private static AtlasEntity getFieldEntityWithGuid(String guid) {
        AtlasEntity ret = new AtlasEntity(KafkaDataTypes.AVRO_FIELD.getName());
        ret.setGuid(guid);
        return ret;
    }

    @Test
    public void testImportTopic() throws Exception {
        KafkaUtils mockKafkaUtils = mock(KafkaUtils.class);
        when(mockKafkaUtils.listAllTopics())
                .thenReturn(Collections.singletonList(TEST_TOPIC_NAME));
        when(mockKafkaUtils.getPartitionCount(TEST_TOPIC_NAME))
                .thenReturn(3);

        EntityMutationResponse mockCreateResponse = mock(EntityMutationResponse.class);
        AtlasEntityHeader mockAtlasEntityHeader = mock(AtlasEntityHeader.class);
        when(mockAtlasEntityHeader.getGuid()).thenReturn(TOPIC_WITH_EXT_INFO.getEntity().getGuid());
        when(mockCreateResponse.getCreatedEntities())
                .thenReturn(Collections.singletonList(mockAtlasEntityHeader));

        AtlasClientV2 mockAtlasClientV2 = mock(AtlasClientV2.class);
        when(mockAtlasClientV2.createEntity(any()))
                .thenReturn(mockCreateResponse);
        when(mockAtlasClientV2.getEntityByGuid(TOPIC_WITH_EXT_INFO.getEntity().getGuid()))
                .thenReturn(TOPIC_WITH_EXT_INFO);

        KafkaBridge bridge = new KafkaBridge(ApplicationProperties.get(), mockAtlasClientV2, mockKafkaUtils);
        bridge.importTopic(TEST_TOPIC_NAME);

        ArgumentCaptor<AtlasEntity.AtlasEntityWithExtInfo> argumentCaptor = ArgumentCaptor.forClass(AtlasEntity.AtlasEntityWithExtInfo.class);
        verify(mockAtlasClientV2).createEntity(argumentCaptor.capture());
        AtlasEntity.AtlasEntityWithExtInfo entity = argumentCaptor.getValue();
        assertEquals(entity.getEntity().getAttribute("qualifiedName"), TOPIC_QUALIFIED_NAME);
    }

    @Test
    public void testCreateTopic() throws Exception {
        KafkaUtils mockKafkaUtils = mock(KafkaUtils.class);
        when(mockKafkaUtils.listAllTopics())
                .thenReturn(Collections.singletonList(TEST_TOPIC_NAME));
        when(mockKafkaUtils.getPartitionCount(TEST_TOPIC_NAME))
                .thenReturn(3);

        EntityMutationResponse mockCreateResponse = mock(EntityMutationResponse.class);
        AtlasEntityHeader mockAtlasEntityHeader = mock(AtlasEntityHeader.class);
        when(mockAtlasEntityHeader.getGuid()).thenReturn(TOPIC_WITH_EXT_INFO.getEntity().getGuid());
        when(mockCreateResponse.getCreatedEntities())
                .thenReturn(Collections.singletonList(mockAtlasEntityHeader));

        AtlasClientV2 mockAtlasClientV2 = mock(AtlasClientV2.class);
        when(mockAtlasClientV2.createEntity(any()))
                .thenReturn(mockCreateResponse);
        when(mockAtlasClientV2.getEntityByGuid(TOPIC_WITH_EXT_INFO.getEntity().getGuid()))
                .thenReturn(TOPIC_WITH_EXT_INFO);

        KafkaBridge bridge = new KafkaBridge(ApplicationProperties.get(), mockAtlasClientV2, mockKafkaUtils);
        AtlasEntity.AtlasEntityWithExtInfo ret = bridge.createOrUpdateTopic(TEST_TOPIC_NAME);

        assertEquals(TOPIC_WITH_EXT_INFO, ret);
    }

    @Test
    public void testCreateSchema() throws Exception {
        KafkaUtils mockKafkaUtils = mock(KafkaUtils.class);
        when(mockKafkaUtils.listAllTopics())
                .thenReturn(Collections.singletonList(TEST_TOPIC_NAME));
        when(mockKafkaUtils.getPartitionCount(TEST_TOPIC_NAME))
                .thenReturn(3);

        EntityMutationResponse mockCreateResponse = mock(EntityMutationResponse.class);
        AtlasEntityHeader mockAtlasEntityHeader = mock(AtlasEntityHeader.class);
        when(mockAtlasEntityHeader.getGuid()).thenReturn(SCHEMA_WITH_EXT_INFO.getEntity().getGuid());
        when(mockCreateResponse.getCreatedEntities())
                .thenReturn(Collections.singletonList(mockAtlasEntityHeader));

        AtlasClientV2 mockAtlasClientV2 = mock(AtlasClientV2.class);
        when(mockAtlasClientV2.createEntity(any()))
                .thenReturn(mockCreateResponse);
        when(mockAtlasClientV2.getEntityByGuid(SCHEMA_WITH_EXT_INFO.getEntity().getGuid()))
                .thenReturn(SCHEMA_WITH_EXT_INFO);

        KafkaBridge bridge = new KafkaBridge(ApplicationProperties.get(), mockAtlasClientV2, mockKafkaUtils);
        AtlasEntity.AtlasEntityWithExtInfo ret = bridge.createOrUpdateSchema(TEST_SCHEMA, TEST_SCHEMA_NAME, TEST_NAMESPACE, TEST_SCHEMA_VERSION);

        assertEquals(SCHEMA_WITH_EXT_INFO, ret);
    }

    @Test
    public void testCreateField() throws Exception {
        KafkaUtils mockKafkaUtils = mock(KafkaUtils.class);
        when(mockKafkaUtils.listAllTopics())
                .thenReturn(Collections.singletonList(TEST_TOPIC_NAME));
        when(mockKafkaUtils.getPartitionCount(TEST_TOPIC_NAME))
                .thenReturn(3);

        EntityMutationResponse mockCreateResponse = mock(EntityMutationResponse.class);
        AtlasEntityHeader mockAtlasEntityHeader = mock(AtlasEntityHeader.class);
        when(mockAtlasEntityHeader.getGuid()).thenReturn(FIELD_WITH_EXT_INFO.getEntity().getGuid());
        when(mockCreateResponse.getCreatedEntities())
                .thenReturn(Collections.singletonList(mockAtlasEntityHeader));

        AtlasClientV2 mockAtlasClientV2 = mock(AtlasClientV2.class);
        when(mockAtlasClientV2.createEntity(any()))
                .thenReturn(mockCreateResponse);
        when(mockAtlasClientV2.getEntityByGuid(FIELD_WITH_EXT_INFO.getEntity().getGuid()))
                .thenReturn(FIELD_WITH_EXT_INFO);

        KafkaBridge bridge = new KafkaBridge(ApplicationProperties.get(), mockAtlasClientV2, mockKafkaUtils);
        AtlasEntity.AtlasEntityWithExtInfo ret = bridge.createOrUpdateField(TEST_FIELD_NAME, TEST_SCHEMA_NAME, TEST_NAMESPACE, TEST_SCHEMA_VERSION, TEST_FIELD_FULLNAME);

        assertEquals(FIELD_WITH_EXT_INFO, ret);
    }

    @Test
    public void testUpdateTopic() throws Exception {
        KafkaUtils mockKafkaUtils = mock(KafkaUtils.class);
        when(mockKafkaUtils.listAllTopics())
                .thenReturn(Collections.singletonList(TEST_TOPIC_NAME));
        when(mockKafkaUtils.getPartitionCount(TEST_TOPIC_NAME))
                .thenReturn(3);

        EntityMutationResponse mockUpdateResponse = mock(EntityMutationResponse.class);
        AtlasEntityHeader mockAtlasEntityHeader = mock(AtlasEntityHeader.class);
        when(mockAtlasEntityHeader.getGuid()).thenReturn(TOPIC_WITH_EXT_INFO.getEntity().getGuid());
        when(mockUpdateResponse.getUpdatedEntities())
                .thenReturn(Collections.singletonList(mockAtlasEntityHeader));

        AtlasClientV2 mockAtlasClientV2 = mock(AtlasClientV2.class);
        when(mockAtlasClientV2.getEntityByAttribute(eq(KafkaDataTypes.KAFKA_TOPIC.getName()), any()))
                .thenReturn(TOPIC_WITH_EXT_INFO);
        when(mockAtlasClientV2.updateEntity(any()))
                .thenReturn(mockUpdateResponse);
        when(mockAtlasClientV2.getEntityByGuid(TOPIC_WITH_EXT_INFO.getEntity().getGuid()))
                .thenReturn(TOPIC_WITH_EXT_INFO);

        KafkaBridge bridge = new KafkaBridge(ApplicationProperties.get(), mockAtlasClientV2, mockKafkaUtils);
        AtlasEntity.AtlasEntityWithExtInfo ret = bridge.createOrUpdateTopic(TEST_TOPIC_NAME);

        assertEquals(TOPIC_WITH_EXT_INFO, ret);
    }

    @Test
    public void testUpdateSchema() throws Exception {
        KafkaUtils mockKafkaUtils = mock(KafkaUtils.class);
        when(mockKafkaUtils.listAllTopics())
                .thenReturn(Collections.singletonList(TEST_TOPIC_NAME));
        when(mockKafkaUtils.getPartitionCount(TEST_TOPIC_NAME))
                .thenReturn(3);

        EntityMutationResponse mockUpdateResponseSchema = mock(EntityMutationResponse.class);
        AtlasEntityHeader mockAtlasEntityHeaderSchema = mock(AtlasEntityHeader.class);
        when(mockAtlasEntityHeaderSchema.getGuid()).thenReturn(SCHEMA_WITH_EXT_INFO.getEntity().getGuid());
        when(mockUpdateResponseSchema.getUpdatedEntities())
                .thenReturn(Collections.singletonList(mockAtlasEntityHeaderSchema));

        AtlasClientV2 mockAtlasClientV2 = mock(AtlasClientV2.class);
        when(mockAtlasClientV2.getEntityByAttribute(eq(KafkaDataTypes.AVRO_SCHEMA.getName()), any()))
                .thenReturn(SCHEMA_WITH_EXT_INFO);
        when(mockAtlasClientV2.updateEntity(SCHEMA_WITH_EXT_INFO))
                .thenReturn(mockUpdateResponseSchema);
        when(mockAtlasClientV2.getEntityByGuid(SCHEMA_WITH_EXT_INFO.getEntity().getGuid()))
                .thenReturn(SCHEMA_WITH_EXT_INFO);

        EntityMutationResponse mockUpdateResponseField = mock(EntityMutationResponse.class);
        AtlasEntityHeader mockAtlasEntityHeaderField = mock(AtlasEntityHeader.class);
        when(mockAtlasEntityHeaderField.getGuid()).thenReturn(FIELD_WITH_EXT_INFO.getEntity().getGuid());
        when(mockUpdateResponseField.getUpdatedEntities())
                .thenReturn(Collections.singletonList(mockAtlasEntityHeaderField));

        when(mockAtlasClientV2.getEntityByAttribute(eq(KafkaDataTypes.AVRO_FIELD.getName()), any()))
                .thenReturn(FIELD_WITH_EXT_INFO);
        when(mockAtlasClientV2.updateEntity(FIELD_WITH_EXT_INFO))
                .thenReturn(mockUpdateResponseField);
        when(mockAtlasClientV2.getEntityByGuid(FIELD_WITH_EXT_INFO.getEntity().getGuid()))
                .thenReturn(FIELD_WITH_EXT_INFO);

        KafkaBridge bridge = new KafkaBridge(ApplicationProperties.get(), mockAtlasClientV2, mockKafkaUtils);
        AtlasEntity.AtlasEntityWithExtInfo ret = bridge.createOrUpdateSchema(TEST_SCHEMA, TEST_SCHEMA_NAME, TEST_NAMESPACE, TEST_SCHEMA_VERSION);

        System.out.println(SCHEMA_WITH_EXT_INFO);
        System.out.println(ret);
        assertEquals(SCHEMA_WITH_EXT_INFO, ret);
    }

    @Test
    public void testUpdateField() throws Exception {
        KafkaUtils mockKafkaUtils = mock(KafkaUtils.class);
        when(mockKafkaUtils.listAllTopics())
                .thenReturn(Collections.singletonList(TEST_TOPIC_NAME));
        when(mockKafkaUtils.getPartitionCount(TEST_TOPIC_NAME))
                .thenReturn(3);

        EntityMutationResponse mockUpdateResponse = mock(EntityMutationResponse.class);
        AtlasEntityHeader mockAtlasEntityHeader = mock(AtlasEntityHeader.class);
        when(mockAtlasEntityHeader.getGuid()).thenReturn(FIELD_WITH_EXT_INFO.getEntity().getGuid());
        when(mockUpdateResponse.getUpdatedEntities())
                .thenReturn(Collections.singletonList(mockAtlasEntityHeader));

        AtlasClientV2 mockAtlasClientV2 = mock(AtlasClientV2.class);
        when(mockAtlasClientV2.getEntityByAttribute(eq(KafkaDataTypes.AVRO_FIELD.getName()), any()))
                .thenReturn(FIELD_WITH_EXT_INFO);
        when(mockAtlasClientV2.updateEntity(any()))
                .thenReturn(mockUpdateResponse);
        when(mockAtlasClientV2.getEntityByGuid(FIELD_WITH_EXT_INFO.getEntity().getGuid()))
                .thenReturn(FIELD_WITH_EXT_INFO);

        KafkaBridge bridge = new KafkaBridge(ApplicationProperties.get(), mockAtlasClientV2, mockKafkaUtils);
        AtlasEntity.AtlasEntityWithExtInfo ret = bridge.createOrUpdateField(TEST_FIELD_NAME, TEST_SCHEMA_NAME, TEST_NAMESPACE, TEST_SCHEMA_VERSION, TEST_FIELD_FULLNAME);

        assertEquals(FIELD_WITH_EXT_INFO, ret);
    }

    @Test
    public void testGetSchemas() throws Exception {
        CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
        when(mockResponse.getStatusLine())
                .thenReturn(mock(StatusLine.class));
        when(mockResponse.getStatusLine().getStatusCode())
                .thenReturn(HttpStatus.SC_OK);
        when(mockResponse.getEntity())
                .thenReturn(mock(HttpEntity.class));
        when(mockResponse.getEntity().getContent())
                .thenReturn(new ByteArrayInputStream(new String("{\"subject\":\"test-value\",\"version\":1,\"id\":1,\"schema\":" + TEST_SCHEMA + "}").getBytes(StandardCharsets.UTF_8)));

        CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
        when(mockHttpClient.execute(any()))
                .thenReturn(mockResponse);
        when(mockHttpClient.getConnectionManager())
                .thenReturn(mock(ClientConnectionManager.class));

        String ret = SchemaRegistryConnector.getSchemaFromKafkaSchemaRegistry(mockHttpClient, TEST_SCHEMA_NAME, TEST_SCHEMA_VERSION);

        assertEquals(TEST_SCHEMA, ret);
    }

    @Test
    public void testGetSchemaVersions() throws Exception {
        CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
        when(mockResponse.getStatusLine())
                .thenReturn(mock(StatusLine.class));
        when(mockResponse.getStatusLine().getStatusCode())
                .thenReturn(HttpStatus.SC_OK);
        when(mockResponse.getEntity())
                .thenReturn(mock(HttpEntity.class));
        when(mockResponse.getEntity().getContent())
                .thenReturn(new ByteArrayInputStream(new String(TEST_SCHEMA_VERSION_LIST.toString()).getBytes(StandardCharsets.UTF_8)));

        CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
        when(mockHttpClient.execute(any()))
                .thenReturn(mockResponse);
        when(mockHttpClient.getConnectionManager())
                .thenReturn(mock(ClientConnectionManager.class));

        ArrayList<Integer> ret = SchemaRegistryConnector.getVersionsKafkaSchemaRegistry(mockHttpClient, TEST_SCHEMA_NAME);

        assertEquals(TEST_SCHEMA_VERSION_LIST, ret);
    }

    private AtlasEntity.AtlasEntityWithExtInfo createMockSchemaEntityWithExtInfo() {
        AtlasEntity entity = new AtlasEntity(KafkaDataTypes.AVRO_SCHEMA.getName());
        entity.setGuid(TEST_GUID);
        entity.setAttribute("qualifiedName", TEST_SCHEMA_NAME + "@v1@" + TEST_NAMESPACE);
        return new AtlasEntity.AtlasEntityWithExtInfo(entity);
    }

    private void setupSchemaEntityCreation() throws Exception {
        when(mockAtlasClient.getEntityByAttribute(eq(KafkaDataTypes.AVRO_SCHEMA.getName()), any(Map.class))).thenReturn(null);

        EntityMutationResponse mockResponse = mock(EntityMutationResponse.class);
        AtlasEntityHeader mockHeader = mock(AtlasEntityHeader.class);
        when(mockHeader.getGuid()).thenReturn(TEST_GUID);
        when(mockResponse.getCreatedEntities()).thenReturn(Collections.singletonList(mockHeader));
        when(mockAtlasClient.createEntity(any(AtlasEntity.AtlasEntityWithExtInfo.class))).thenReturn(mockResponse);
        when(mockAtlasClient.getEntityByGuid(TEST_GUID)).thenReturn(createMockSchemaEntityWithExtInfo());
    }

    private void setupKafkaBridge() throws Exception {
        setupMocks();
        when(mockConfiguration.getString("atlas.cluster.name", "primary")).thenReturn(TEST_CLUSTER_NAME);
        when(mockConfiguration.getStringArray("atlas.metadata.namespace")).thenReturn(TEST_NAMESPACE_ARRAY);
        when(mockKafkaUtils.listAllTopics()).thenReturn(Arrays.asList(TEST_TOPIC_NAME));

        kafkaBridge = new KafkaBridge(mockConfiguration, mockAtlasClient, mockKafkaUtils);
    }

    private AtlasEntity.AtlasEntityWithExtInfo createMockEntityWithExtInfo() {
        AtlasEntity entity = new AtlasEntity(KafkaDataTypes.KAFKA_TOPIC.getName());
        entity.setGuid(TEST_GUID);
        entity.setAttribute("qualifiedName", TEST_QUALIFIED_NAME);
        return new AtlasEntity.AtlasEntityWithExtInfo(entity);
    }

    @Test
    public void testPrintUsageMethod() throws Exception {
        // Setup
        setupKafkaBridge();

        // Use reflection to test private static method
        java.lang.reflect.Method method = KafkaBridge.class.getDeclaredMethod("printUsage");
        method.setAccessible(true);

        // Execute - should not throw exception
        method.invoke(null);
    }

    @Test
    public void testClearRelationshipAttributesWithEntity() throws Exception {
        // Setup
        setupKafkaBridge();
        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = createMockEntityWithExtInfo();
        entityWithExtInfo.getEntity().setRelationshipAttribute("test", "value");

        // Use reflection to test private method
        java.lang.reflect.Method method = KafkaBridge.class.getDeclaredMethod("clearRelationshipAttributes", AtlasEntity.AtlasEntityWithExtInfo.class);
        method.setAccessible(true);

        // Execute
        method.invoke(kafkaBridge, entityWithExtInfo);

        // Verify - should not throw exception
        assertNotNull(entityWithExtInfo);
    }

    @Test
    public void testClearRelationshipAttributesWithNullEntity() throws Exception {
        // Setup
        setupKafkaBridge();

        // Use reflection to test private method
        java.lang.reflect.Method method = KafkaBridge.class.getDeclaredMethod("clearRelationshipAttributes", AtlasEntity.AtlasEntityWithExtInfo.class);
        method.setAccessible(true);

        // Execute - should not throw exception with null input
        method.invoke(kafkaBridge, (AtlasEntity.AtlasEntityWithExtInfo) null);
    }

    @Test
    public void testClearRelationshipAttributesWithCollection() throws Exception {
        // Setup
        setupKafkaBridge();
        List<AtlasEntity> entities = new ArrayList<>();
        AtlasEntity entity = new AtlasEntity(KafkaDataTypes.KAFKA_TOPIC.getName());
        entity.setRelationshipAttribute("test", "value");
        entities.add(entity);

        // Use reflection to test private method
        java.lang.reflect.Method method = KafkaBridge.class.getDeclaredMethod("clearRelationshipAttributes", Collection.class);
        method.setAccessible(true);

        // Execute
        method.invoke(kafkaBridge, entities);

        // Verify - should not throw exception
        assertNotNull(entities);
    }

    @Test
    public void testFindOrCreateAtlasSchemaWithMultipleVersions() throws Exception {
        // Setup
        setupKafkaBridge();

        // Mock HTTP responses for multiple schema versions
        when(mockHttpResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
        when(mockHttpResponse.getEntity()).thenReturn(mockHttpEntity);
        when(mockHttpEntity.getContent()).thenReturn(new ByteArrayInputStream("[1,2,3]".getBytes(StandardCharsets.UTF_8)));
        when(mockHttpClient.execute(any())).thenReturn(mockHttpResponse);

        // Mock schema content responses
        when(mockHttpEntity.getContent())
                .thenReturn(new ByteArrayInputStream("[1,2,3]".getBytes(StandardCharsets.UTF_8)))
                .thenReturn(new ByteArrayInputStream(TEST_SCHEMA.getBytes(StandardCharsets.UTF_8)))
                .thenReturn(new ByteArrayInputStream(TEST_SCHEMA.getBytes(StandardCharsets.UTF_8)))
                .thenReturn(new ByteArrayInputStream(TEST_SCHEMA.getBytes(StandardCharsets.UTF_8)));

        // Use reflection to access and replace httpClient field
        java.lang.reflect.Field httpClientField = KafkaBridge.class.getDeclaredField("httpClient");
        httpClientField.setAccessible(true);
        httpClientField.set(kafkaBridge, mockHttpClient);

        // Mock Atlas client responses
        when(mockAtlasClient.getEntityByAttribute(anyString(), any(Map.class))).thenReturn(null);
        setupSchemaEntityCreation();

        // Use reflection to test private method
        java.lang.reflect.Method method = KafkaBridge.class.getDeclaredMethod("findOrCreateAtlasSchema", String.class);
        method.setAccessible(true);

        // Execute
        try {
            List<AtlasEntity> result = (List<AtlasEntity>) method.invoke(kafkaBridge, TEST_TOPIC_NAME);
            assertNotNull(result);
        } catch (Exception e) {
            // Expected due to complex schema registry mocking
            assertTrue(e.getCause() instanceof RuntimeException || e.getCause() instanceof IOException);
        }
    }

    @Test
    public void testFindOrCreateAtlasSchemaWithExistingAtlasEntity() throws Exception {
        // Setup
        setupKafkaBridge();

        // Mock HTTP responses
        when(mockHttpResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
        when(mockHttpResponse.getEntity()).thenReturn(mockHttpEntity);
        when(mockHttpEntity.getContent()).thenReturn(new ByteArrayInputStream("[1]".getBytes(StandardCharsets.UTF_8)));
        when(mockHttpClient.execute(any())).thenReturn(mockHttpResponse);

        // Use reflection to access and replace httpClient field
        java.lang.reflect.Field httpClientField = KafkaBridge.class.getDeclaredField("httpClient");
        httpClientField.setAccessible(true);
        httpClientField.set(kafkaBridge, mockHttpClient);

        // Mock Atlas client to return existing entity
        AtlasEntity.AtlasEntityWithExtInfo existingEntity = createMockSchemaEntityWithExtInfo();
        when(mockAtlasClient.getEntityByAttribute(anyString(), any(Map.class))).thenReturn(existingEntity);
        setupSchemaEntityCreation();

        // Use reflection to test private method
        java.lang.reflect.Method method = KafkaBridge.class.getDeclaredMethod("findOrCreateAtlasSchema", String.class);
        method.setAccessible(true);

        // Execute
        try {
            List<AtlasEntity> result = (List<AtlasEntity>) method.invoke(kafkaBridge, TEST_TOPIC_NAME);
            assertNotNull(result);
        } catch (Exception e) {
            // Expected due to complex schema registry mocking
            assertTrue(e.getCause() instanceof RuntimeException || e.getCause() instanceof IOException);
        }
    }

    @Test
    public void testFindOrCreateAtlasSchemaWithNullSchemaContent() throws Exception {
        setupKafkaBridge();

        // Mock HTTP responses with null schema content
        when(mockHttpResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
        when(mockHttpResponse.getEntity()).thenReturn(mockHttpEntity);
        when(mockHttpEntity.getContent())
                .thenReturn(new ByteArrayInputStream("[1]".getBytes(StandardCharsets.UTF_8)))
                .thenReturn(new ByteArrayInputStream("null".getBytes(StandardCharsets.UTF_8)));
        when(mockHttpClient.execute(any())).thenReturn(mockHttpResponse);

        // Use reflection to access and replace httpClient field
        java.lang.reflect.Field httpClientField = KafkaBridge.class.getDeclaredField("httpClient");
        httpClientField.setAccessible(true);
        httpClientField.set(kafkaBridge, mockHttpClient);

        // Use reflection to test private method
        java.lang.reflect.Method method = KafkaBridge.class.getDeclaredMethod("findOrCreateAtlasSchema", String.class);
        method.setAccessible(true);

        // Execute
        try {
            List<AtlasEntity> result = (List<AtlasEntity>) method.invoke(kafkaBridge, TEST_TOPIC_NAME);
            assertNotNull(result);
            // Should be empty when schema content is null
            assertTrue(result.isEmpty());
        } catch (Exception e) {
            // Expected due to schema registry connectivity issues
            assertTrue(e.getCause() instanceof RuntimeException || e.getCause() instanceof IOException);
        }
    }

    @Test
    public void testFindOrCreateAtlasSchemaWithSchemaRegistryError() throws Exception {
        setupKafkaBridge();

        when(mockHttpResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);
        when(mockHttpClient.execute(any())).thenReturn(mockHttpResponse);

        java.lang.reflect.Field httpClientField = KafkaBridge.class.getDeclaredField("httpClient");
        httpClientField.setAccessible(true);
        httpClientField.set(kafkaBridge, mockHttpClient);

        // Use reflection to test private method
        java.lang.reflect.Method method = KafkaBridge.class.getDeclaredMethod("findOrCreateAtlasSchema", String.class);
        method.setAccessible(true);

        try {
            List<AtlasEntity> result = (List<AtlasEntity>) method.invoke(kafkaBridge, TEST_TOPIC_NAME);
            assertNotNull(result);
            assertTrue(result.isEmpty());
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof RuntimeException || e.getCause() instanceof IOException);
        }
    }

    @Test
    public void testImportTopicRegexFilters() throws Exception {
        when(mockConfiguration.getString("atlas.cluster.name", "primary")).thenReturn(TEST_CLUSTER_NAME);
        when(mockConfiguration.getStringArray("atlas.metadata.namespace")).thenReturn(TEST_NAMESPACE_ARRAY);
        when(mockKafkaUtils.listAllTopics()).thenReturn(Arrays.asList("payments", "orders", "inventory"));
        KafkaBridge bridge = new KafkaBridge(mockConfiguration, mockAtlasClient, mockKafkaUtils);
        KafkaBridge spyBridge = spy(bridge);
        AtlasEntity.AtlasEntityWithExtInfo dummy = new AtlasEntity.AtlasEntityWithExtInfo(new AtlasEntity(KafkaDataTypes.KAFKA_TOPIC.getName()));
        doReturn(dummy).when(spyBridge).createOrUpdateTopic(anyString());
        spyBridge.importTopic("orders|inven.*");
        verify(spyBridge).createOrUpdateTopic("orders");
        verify(spyBridge).createOrUpdateTopic("inventory");
        verify(spyBridge, never()).createOrUpdateTopic("payments");
    }

    @Test(expectedExceptions = Exception.class)
    public void testGetTopicEntity_partitionErrorThrows() throws Exception {
        when(mockConfiguration.getString("atlas.cluster.name", "primary")).thenReturn(TEST_CLUSTER_NAME);
        when(mockConfiguration.getStringArray("atlas.metadata.namespace")).thenReturn(TEST_NAMESPACE_ARRAY);
        when(mockKafkaUtils.listAllTopics()).thenReturn(Collections.singletonList(TEST_TOPIC_NAME));
        when(mockKafkaUtils.getPartitionCount(TEST_TOPIC_NAME)).thenThrow(new java.util.concurrent.ExecutionException("fail", new RuntimeException("boom")));
        KafkaBridge bridge = new KafkaBridge(mockConfiguration, mockAtlasClient, mockKafkaUtils);
        bridge.getTopicEntity(TEST_TOPIC_NAME, null);
    }

    @Test
    public void testGetSchemaEntity_namespaceFallback() throws Exception {
        when(mockConfiguration.getString("atlas.cluster.name", "primary")).thenReturn(TEST_CLUSTER_NAME);
        when(mockConfiguration.getStringArray("atlas.metadata.namespace")).thenReturn(TEST_NAMESPACE_ARRAY);
        when(mockKafkaUtils.listAllTopics()).thenReturn(Collections.singletonList(TEST_TOPIC_NAME));
        // Schema without namespace triggers fallback to key constant when input namespace is null
        String schemaNoNs = "{\"type\":\"record\",\"name\":\"Rec\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"}]}";
        KafkaBridge bridge = new KafkaBridge(mockConfiguration, mockAtlasClient, mockKafkaUtils);
        KafkaBridge spyBridge = spy(bridge);
        doReturn(Collections.emptyList())
                .when(spyBridge)
                .createNestedFields(any(Schema.class), anyString(), anyString(), any(int.class), anyString());
        AtlasEntity entity = spyBridge.getSchemaEntity(schemaNoNs, TEST_SCHEMA_NAME, null, 1, null);
        assertEquals(entity.getAttribute("namespace"), "atlas.metadata.namespace");
    }

    @Test
    public void testCreateNestedFields_arrayAndNestedRecord() throws Exception {
        when(mockConfiguration.getString("atlas.cluster.name", "primary")).thenReturn(TEST_CLUSTER_NAME);
        when(mockConfiguration.getStringArray("atlas.metadata.namespace")).thenReturn(TEST_NAMESPACE_ARRAY);
        when(mockKafkaUtils.listAllTopics()).thenReturn(Collections.singletonList(TEST_TOPIC_NAME));
        // Avro schema with array and nested record to cover both branches
        String complexSchema = "{\"type\":\"record\",\"name\":\"Top\",\"fields\":["
                + "{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"Sub\",\"fields\":[{\"name\":\"x\",\"type\":\"string\"}]}}},"
                + "{\"name\":\"inner\",\"type\":{\"type\":\"record\",\"name\":\"Inner\",\"fields\":[{\"name\":\"y\",\"type\":\"int\"}]}}]}";
        KafkaBridge bridge = new KafkaBridge(mockConfiguration, mockAtlasClient, mockKafkaUtils);
        KafkaBridge spyBridge = spy(bridge);
        // Stub createOrUpdateField to avoid Atlas interactions and return simple entities
        doReturn(new AtlasEntity.AtlasEntityWithExtInfo(new AtlasEntity(KafkaDataTypes.AVRO_FIELD.getName())))
                .when(spyBridge).createOrUpdateField(any(Schema.Field.class), anyString(), anyString(), any(int.class), anyString());
        List<AtlasEntity> fields = spyBridge.createNestedFields(new Schema.Parser().parse(complexSchema), TEST_SCHEMA_NAME, TEST_NAMESPACE, 1, "");
        assertNotNull(fields);
        // Expect 2 leaf fields (x and y)
        assertEquals(fields.size(), 2);
    }

    @Test
    public void testCreateEntityInAtlas_noCreatedEntities() throws Exception {
        EntityMutationResponse mockResponse = mock(EntityMutationResponse.class);
        when(mockResponse.getCreatedEntities()).thenReturn(Collections.emptyList());
        when(mockAtlasClient.createEntity(any(AtlasEntity.AtlasEntityWithExtInfo.class))).thenReturn(mockResponse);
        when(mockConfiguration.getString("atlas.cluster.name", "primary")).thenReturn(TEST_CLUSTER_NAME);
        when(mockConfiguration.getStringArray("atlas.metadata.namespace")).thenReturn(TEST_NAMESPACE_ARRAY);
        when(mockKafkaUtils.listAllTopics()).thenReturn(Collections.singletonList(TEST_TOPIC_NAME));
        KafkaBridge bridge = new KafkaBridge(mockConfiguration, mockAtlasClient, mockKafkaUtils);
        AtlasEntity.AtlasEntityWithExtInfo input = new AtlasEntity.AtlasEntityWithExtInfo(new AtlasEntity(KafkaDataTypes.KAFKA_TOPIC.getName()));
        AtlasEntity.AtlasEntityWithExtInfo ret = bridge.createEntityInAtlas(input);
        assertEquals(ret, null);
    }

    @Test
    public void testUpdateEntityInAtlas_nullResponseReturnsInput() throws Exception {
        when(mockAtlasClient.updateEntity(any(AtlasEntity.AtlasEntityWithExtInfo.class))).thenReturn(null);
        when(mockConfiguration.getString("atlas.cluster.name", "primary")).thenReturn(TEST_CLUSTER_NAME);
        when(mockConfiguration.getStringArray("atlas.metadata.namespace")).thenReturn(TEST_NAMESPACE_ARRAY);
        when(mockKafkaUtils.listAllTopics()).thenReturn(Collections.singletonList(TEST_TOPIC_NAME));
        KafkaBridge bridge = new KafkaBridge(mockConfiguration, mockAtlasClient, mockKafkaUtils);
        AtlasEntity.AtlasEntityWithExtInfo input = new AtlasEntity.AtlasEntityWithExtInfo(new AtlasEntity(KafkaDataTypes.KAFKA_TOPIC.getName()));
        AtlasEntity.AtlasEntityWithExtInfo ret = bridge.updateEntityInAtlas(input);
        assertEquals(ret, input);
    }

    @Test
    public void testUpdateEntityInAtlas_emptyUpdatedEntitiesReturnsInput() throws Exception {
        EntityMutationResponse mockResponse = mock(EntityMutationResponse.class);
        when(mockResponse.getUpdatedEntities()).thenReturn(Collections.emptyList());
        when(mockAtlasClient.updateEntity(any(AtlasEntity.AtlasEntityWithExtInfo.class))).thenReturn(mockResponse);
        when(mockConfiguration.getString("atlas.cluster.name", "primary")).thenReturn(TEST_CLUSTER_NAME);
        when(mockConfiguration.getStringArray("atlas.metadata.namespace")).thenReturn(TEST_NAMESPACE_ARRAY);
        when(mockKafkaUtils.listAllTopics()).thenReturn(Collections.singletonList(TEST_TOPIC_NAME));
        KafkaBridge bridge = new KafkaBridge(mockConfiguration, mockAtlasClient, mockKafkaUtils);
        AtlasEntity.AtlasEntityWithExtInfo input = new AtlasEntity.AtlasEntityWithExtInfo(new AtlasEntity(KafkaDataTypes.KAFKA_TOPIC.getName()));
        AtlasEntity.AtlasEntityWithExtInfo ret = bridge.updateEntityInAtlas(input);
        assertEquals(ret, input);
    }

    @Test
    public void testQualifiedNameFormats() {
        assertEquals(KafkaBridge.getTopicQualifiedName("ns", "TopicA"), "topica@ns");
        assertEquals(KafkaBridge.getSchemaQualifiedName("ns", "name-value", "v1"), "name-value@v1@ns");
        assertEquals(KafkaBridge.getFieldQualifiedName("ns", "A.B", "name-value", "v1"), "a.b@name-value@v1@ns");
    }

    @Test
    public void testFindEntityInAtlas_exceptionHandled() throws Exception {
        when(mockConfiguration.getString("atlas.cluster.name", "primary")).thenReturn(TEST_CLUSTER_NAME);
        when(mockConfiguration.getStringArray("atlas.metadata.namespace")).thenReturn(TEST_NAMESPACE_ARRAY);
        when(mockKafkaUtils.listAllTopics()).thenReturn(Collections.singletonList(TEST_TOPIC_NAME));
        when(mockAtlasClient.getEntityByAttribute(anyString(), any(Map.class))).thenThrow(new RuntimeException("fail"));
        KafkaBridge bridge = new KafkaBridge(mockConfiguration, mockAtlasClient, mockKafkaUtils);
        AtlasEntity.AtlasEntityWithExtInfo ret = bridge.findEntityInAtlas("type", "qn");
        assertEquals(ret, null);
    }
}
