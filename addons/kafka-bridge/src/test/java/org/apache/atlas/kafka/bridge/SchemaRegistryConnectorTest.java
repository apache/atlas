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
package org.apache.atlas.kafka.bridge;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class SchemaRegistryConnectorTest {
    @Mock
    private CloseableHttpClient mockHttpClient;

    @Mock
    private CloseableHttpResponse mockHttpResponse;

    @Mock
    private StatusLine mockStatusLine;

    @Mock
    private HttpEntity mockHttpEntity;

    private static final String TEST_SUBJECT = "test-subject";
    private static final int TEST_VERSION = 1;
    private static final String TEST_HOSTNAME = "test-registry-host:8081";

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        setKafkaSchemaRegistryHostname(TEST_HOSTNAME);
        when(mockHttpResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockHttpResponse.getEntity()).thenReturn(mockHttpEntity);
    }

    private void setKafkaSchemaRegistryHostname(String hostname) throws Exception {
        Field field = KafkaBridge.class.getDeclaredField("kafkaSchemaRegistryHostname");
        field.setAccessible(true);
        field.set(null, hostname);
    }

    @Test
    public void testGetVersionsKafkaSchemaRegistry_Success() throws IOException {
        // Arrange
        String jsonResponse = "[1, 2, 3]";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(jsonResponse.getBytes(StandardCharsets.UTF_8));

        when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockHttpResponse);
        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
        when(mockHttpEntity.getContent()).thenReturn(inputStream);

        ArrayList<Integer> result = SchemaRegistryConnector.getVersionsKafkaSchemaRegistry(mockHttpClient, TEST_SUBJECT);

        assertNotNull(result);
        assertEquals(result.size(), 3);
        assertEquals(result.get(0), Integer.valueOf(1));
        assertEquals(result.get(1), Integer.valueOf(2));
        assertEquals(result.get(2), Integer.valueOf(3));

        verify(mockHttpClient).execute(any(HttpGet.class));
        verify(mockHttpResponse).close();
    }

    @Test
    public void testGetVersionsKafkaSchemaRegistry_NotFound() throws IOException {
        // Arrange
        when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockHttpResponse);
        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);

        // Act
        ArrayList<Integer> result = SchemaRegistryConnector.getVersionsKafkaSchemaRegistry(mockHttpClient, TEST_SUBJECT);

        // Assert
        assertNotNull(result);
        assertTrue(result.isEmpty());

        verify(mockHttpClient).execute(any(HttpGet.class));
        verify(mockHttpResponse).close();
    }

    @Test
    public void testGetVersionsKafkaSchemaRegistry_ConnectionError() throws IOException {
        // Arrange
        when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockHttpResponse);
        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_INTERNAL_SERVER_ERROR);

        // Act
        ArrayList<Integer> result = SchemaRegistryConnector.getVersionsKafkaSchemaRegistry(mockHttpClient, TEST_SUBJECT);

        // Assert
        assertNotNull(result);
        assertTrue(result.isEmpty());

        verify(mockHttpClient).execute(any(HttpGet.class));
        verify(mockHttpResponse).close();
    }

    @Test
    public void testGetVersionsKafkaSchemaRegistry_ParseException() throws IOException {
        // Arrange
        String invalidJsonResponse = "invalid json";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(invalidJsonResponse.getBytes(StandardCharsets.UTF_8));

        when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockHttpResponse);
        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
        when(mockHttpEntity.getContent()).thenReturn(inputStream);

        // Act
        ArrayList<Integer> result = SchemaRegistryConnector.getVersionsKafkaSchemaRegistry(mockHttpClient, TEST_SUBJECT);

        // Assert
        assertNotNull(result);
        assertTrue(result.isEmpty());

        verify(mockHttpClient).execute(any(HttpGet.class));
        verify(mockHttpResponse).close();
    }

    @Test
    public void testGetVersionsKafkaSchemaRegistry_IOException() throws IOException {
        // Arrange
        when(mockHttpClient.execute(any(HttpGet.class))).thenThrow(new IOException("Connection failed"));

        // Act
        ArrayList<Integer> result = SchemaRegistryConnector.getVersionsKafkaSchemaRegistry(mockHttpClient, TEST_SUBJECT);

        // Assert
        assertNotNull(result);
        assertTrue(result.isEmpty());

        verify(mockHttpClient).execute(any(HttpGet.class));
    }

    @Test
    public void testGetVersionsKafkaSchemaRegistry_LargeVersionsList() throws IOException {
        // Arrange
        String jsonResponse = "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(jsonResponse.getBytes(StandardCharsets.UTF_8));

        when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockHttpResponse);
        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
        when(mockHttpEntity.getContent()).thenReturn(inputStream);

        // Act
        ArrayList<Integer> result = SchemaRegistryConnector.getVersionsKafkaSchemaRegistry(mockHttpClient, TEST_SUBJECT);

        // Assert
        assertNotNull(result);
        assertEquals(result.size(), 10);
        assertEquals(result.get(0), Integer.valueOf(1));
        assertEquals(result.get(9), Integer.valueOf(10));

        verify(mockHttpClient).execute(any(HttpGet.class));
        verify(mockHttpResponse).close();
    }

    @Test
    public void testGetVersionsKafkaSchemaRegistry_EmptyVersionsList() throws IOException {
        // Arrange
        String jsonResponse = "[]";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(jsonResponse.getBytes(StandardCharsets.UTF_8));

        when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockHttpResponse);
        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
        when(mockHttpEntity.getContent()).thenReturn(inputStream);

        // Act
        ArrayList<Integer> result = SchemaRegistryConnector.getVersionsKafkaSchemaRegistry(mockHttpClient, TEST_SUBJECT);

        // Assert
        assertNotNull(result);
        assertTrue(result.isEmpty());

        verify(mockHttpClient).execute(any(HttpGet.class));
        verify(mockHttpResponse).close();
    }

    @Test
    public void testGetSchemaFromKafkaSchemaRegistry_Success() throws IOException {
        String expectedSchema = "{\"type\":\"record\",\"name\":\"TestRecord\"}";
        String arrangedExpectedSchema = expectedSchema.replace("\"", "\\\"");
        String jsonResponse = "{\"schema\":\"" + arrangedExpectedSchema + "\"}";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(jsonResponse.getBytes(StandardCharsets.UTF_8));

        when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockHttpResponse);
        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
        when(mockHttpEntity.getContent()).thenReturn(inputStream);

        String result = SchemaRegistryConnector.getSchemaFromKafkaSchemaRegistry(mockHttpClient, TEST_SUBJECT, TEST_VERSION);

        assertNotNull(result);
        assertEquals(result, expectedSchema);

        verify(mockHttpClient).execute(any(HttpGet.class));
        verify(mockHttpResponse).close();
    }

    @Test
    public void testGetSchemaFromKafkaSchemaRegistry_NotFound() throws IOException {
        // Arrange
        when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockHttpResponse);
        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);

        String result = SchemaRegistryConnector.getSchemaFromKafkaSchemaRegistry(mockHttpClient, TEST_SUBJECT, TEST_VERSION);

        assertNull(result);

        verify(mockHttpClient).execute(any(HttpGet.class));
        verify(mockHttpResponse).close();
    }

    @Test
    public void testGetSchemaFromKafkaSchemaRegistry_ConnectionError() throws IOException {
        // Arrange
        when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockHttpResponse);
        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_INTERNAL_SERVER_ERROR);

        String result = SchemaRegistryConnector.getSchemaFromKafkaSchemaRegistry(mockHttpClient, TEST_SUBJECT, TEST_VERSION);

        assertNull(result);

        verify(mockHttpClient).execute(any(HttpGet.class));
        verify(mockHttpResponse).close();
    }

    @Test
    public void testGetSchemaFromKafkaSchemaRegistry_ParseException() throws IOException {
        String invalidJsonResponse = "invalid json";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(invalidJsonResponse.getBytes(StandardCharsets.UTF_8));

        when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockHttpResponse);
        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
        when(mockHttpEntity.getContent()).thenReturn(inputStream);

        String result = SchemaRegistryConnector.getSchemaFromKafkaSchemaRegistry(mockHttpClient, TEST_SUBJECT, TEST_VERSION);

        assertNull(result);

        verify(mockHttpClient).execute(any(HttpGet.class));
        verify(mockHttpResponse).close();
    }
}
