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

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.kafka.NotificationProvider;
import org.apache.atlas.model.impexp.AtlasAsyncImportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.notification.MessageSource;
import org.apache.atlas.notification.NotificationException;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.atlas.repository.impexp.AsyncImportService;
import org.apache.atlas.repository.store.graph.v2.asyncimport.ImportTaskListener;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;

public class AsyncImportTaskExecutorTest {
    @Mock
    private AsyncImportService importService;

    @Mock
    private NotificationInterface notificationInterface;

    @Mock
    private ImportTaskListener importTaskListener;

    @Mock
    private MessageSource messageSource;

    private AsyncImportTaskExecutor asyncImportTaskExecutor;

    @BeforeMethod
    public void setup() throws AtlasBaseException {
        MockitoAnnotations.openMocks(this);

        try (MockedStatic<NotificationProvider> mockedStatic = Mockito.mockStatic(NotificationProvider.class)) {
            mockedStatic.when(NotificationProvider::get).thenReturn(notificationInterface);

            when(messageSource.getSource()).thenReturn("AsyncImportTaskPublisher");

            asyncImportTaskExecutor = new AsyncImportTaskExecutor(importService, importTaskListener);
        }
    }

    @Test
    void testRunSuccess() throws AtlasBaseException {
        // Arrange
        AtlasImportResult mockResult = mock(AtlasImportResult.class);
        EntityImportStream mockEntityImportStream = mock(EntityImportStream.class);
        when(mockEntityImportStream.getMd5Hash()).thenReturn("import-md5-hash");
        when(mockEntityImportStream.size()).thenReturn(5);
        when(mockEntityImportStream.getCreationOrder()).thenReturn(Collections.emptyList());
        when(mockEntityImportStream.hasNext()).thenReturn(false);

        when(importService.fetchImportRequestByImportId("import-md5-hash")).thenReturn(null);
        doNothing().when(importService).saveImportRequest(any(AtlasAsyncImportRequest.class));

        // Act
        AtlasAsyncImportRequest result = asyncImportTaskExecutor.run(mockResult, mockEntityImportStream);

        // Assert
        assertNotNull(result);
        assertSame(result.getStatus(), AtlasAsyncImportRequest.ImportStatus.STAGING);
        verify(mockEntityImportStream).close();
        verify(importService).saveImportRequest(any(AtlasAsyncImportRequest.class));
    }

    @Test
    void testRunDuplicateRequestInWaitingStatus() throws AtlasBaseException {
        // Arrange
        AtlasImportResult mockResult = mock(AtlasImportResult.class);
        EntityImportStream mockEntityImportStream = mock(EntityImportStream.class);
        when(mockEntityImportStream.getMd5Hash()).thenReturn("import-md5");
        when(mockEntityImportStream.size()).thenReturn(10);
        when(mockEntityImportStream.getCreationOrder()).thenReturn(Collections.emptyList());

        // Create mock request in WAITING state
        AtlasAsyncImportRequest mockRequest = mock(AtlasAsyncImportRequest.class);
        when(mockRequest.getStatus()).thenReturn(AtlasAsyncImportRequest.ImportStatus.WAITING);
        when(importService.fetchImportRequestByImportId("import-md5")).thenReturn(mockRequest);

        doNothing().when(mockEntityImportStream).close();

        // Act
        AtlasAsyncImportRequest result = asyncImportTaskExecutor.run(mockResult, mockEntityImportStream);

        // Assert
        assertNotNull(result);
        assertSame(result.getStatus(), AtlasAsyncImportRequest.ImportStatus.WAITING);
        verify(mockEntityImportStream).close();
        verify(importService, never()).saveImportRequest(any());
        verify(importService, never()).updateImportRequest(any());

        // Verify that skipToPosition and publishImportRequest are NOT called
        AsyncImportTaskExecutor spyPublisher = spy(asyncImportTaskExecutor);
        doNothing().when(spyPublisher).skipToPosition(any(), any());
        doNothing().when(spyPublisher).publishImportRequest(any(), any());

        verify(spyPublisher, never()).skipToPosition(mockRequest, mockEntityImportStream);
        verify(spyPublisher, never()).publishImportRequest(mockRequest, mockEntityImportStream);
    }

    @Test
    void testRunDuplicateRequestInProcessingStatus() throws AtlasBaseException {
        // Arrange
        AtlasImportResult mockResult = mock(AtlasImportResult.class);
        EntityImportStream mockEntityImportStream = mock(EntityImportStream.class);
        when(mockEntityImportStream.getMd5Hash()).thenReturn("import-md5");
        when(mockEntityImportStream.size()).thenReturn(10);
        when(mockEntityImportStream.getCreationOrder()).thenReturn(Collections.emptyList());

        // Create mock request in WAITING state
        AtlasAsyncImportRequest mockRequest = mock(AtlasAsyncImportRequest.class);
        when(mockRequest.getStatus()).thenReturn(AtlasAsyncImportRequest.ImportStatus.PROCESSING);
        when(importService.fetchImportRequestByImportId("import-md5")).thenReturn(mockRequest);

        doNothing().when(mockEntityImportStream).close();

        // Act
        AtlasAsyncImportRequest result = asyncImportTaskExecutor.run(mockResult, mockEntityImportStream);

        // Assert
        assertNotNull(result);
        assertSame(result.getStatus(), AtlasAsyncImportRequest.ImportStatus.PROCESSING);
        verify(mockEntityImportStream).close();
        verify(importService, never()).saveImportRequest(any());
        verify(importService, never()).updateImportRequest(any());

        // Verify that skipToPosition and publishImportRequest are NOT called
        AsyncImportTaskExecutor spyPublisher = spy(asyncImportTaskExecutor);
        doNothing().when(spyPublisher).skipToPosition(any(), any());
        doNothing().when(spyPublisher).publishImportRequest(any(), any());

        verify(spyPublisher, never()).skipToPosition(mockRequest, mockEntityImportStream);
        verify(spyPublisher, never()).publishImportRequest(mockRequest, mockEntityImportStream);
    }

    @Test
    void testPublishImportRequestHappyPath() throws AtlasBaseException {
        // Arrange
        AtlasAsyncImportRequest mockImportRequest = mock(AtlasAsyncImportRequest.class);
        AtlasImportResult mockResult = mock(AtlasImportResult.class);
        EntityImportStream mockEntityImportStream = mock(EntityImportStream.class);
        when(mockImportRequest.getTopicName()).thenReturn("test-topic");
        when(mockImportRequest.getImportId()).thenReturn("import-md5");
        when(mockImportRequest.getImportDetails()).thenReturn(new AtlasAsyncImportRequest.ImportDetails());
        when(mockImportRequest.getAtlasImportResult()).thenReturn(mockResult);
        when(mockResult.getUserName()).thenReturn("test-user-1");
        when(mockEntityImportStream.getTypesDef()).thenReturn(null);

        // Act
        asyncImportTaskExecutor.publishImportRequest(mockImportRequest, mockEntityImportStream);

        // Assert
        verify(importService).updateImportRequest(mockImportRequest);
        verify(notificationInterface).closeProducer(NotificationInterface.NotificationType.ASYNC_IMPORT, "test-topic");
        verify(importTaskListener).onReceiveImportRequest(mockImportRequest);
    }

    @Test
    void testPublishImportRequestTypeDefNotificationException() throws AtlasBaseException, NotificationException {
        // Arrange
        AtlasAsyncImportRequest mockImportRequest = mock(AtlasAsyncImportRequest.class);
        AtlasImportResult mockResult = mock(AtlasImportResult.class);
        EntityImportStream mockEntityImportStream = mock(EntityImportStream.class);
        when(mockImportRequest.getTopicName()).thenReturn("test-topic");
        when(mockImportRequest.getImportId()).thenReturn("import-md5");
        when(mockImportRequest.getAtlasImportResult()).thenReturn(mockResult);
        when(mockResult.getUserName()).thenReturn("test-user-1");
        when(mockEntityImportStream.getTypesDef()).thenReturn(null);

        doThrow(new NotificationException(new Exception("some notification exception")))
                .when(notificationInterface)
                .send(eq("test-topic"), anyList(), any());

        // Act
        try {
            asyncImportTaskExecutor.publishImportRequest(mockImportRequest, mockEntityImportStream);
        } catch (AtlasBaseException ignored) {
            // Ignored for this test
        }

        // Assert
        verify(importTaskListener, never()).onReceiveImportRequest(any(AtlasAsyncImportRequest.class));
        verify(notificationInterface).closeProducer(NotificationInterface.NotificationType.ASYNC_IMPORT, "test-topic");
    }

    @Test
    void testPublishEntityNotificationHappyPath() throws AtlasBaseException, NotificationException {
        // Arrange
        AtlasAsyncImportRequest mockImportRequest = mock(AtlasAsyncImportRequest.class);
        AtlasImportResult mockResult = mock(AtlasImportResult.class);
        EntityImportStream mockEntityImportStream = mock(EntityImportStream.class);
        AtlasEntity.AtlasEntityWithExtInfo mockEntityWithExtInfo = mock(AtlasEntity.AtlasEntityWithExtInfo.class);

        when(mockImportRequest.getTopicName()).thenReturn("test-topic");
        when(mockImportRequest.getImportId()).thenReturn("import-id");
        when(mockImportRequest.getImportDetails()).thenReturn(new AtlasAsyncImportRequest.ImportDetails());
        when(mockImportRequest.getAtlasImportResult()).thenReturn(mockResult);
        when(mockResult.getUserName()).thenReturn("test-user-1");
        when(mockEntityImportStream.hasNext()).thenReturn(true, false); // One entity in the stream
        when(mockEntityImportStream.getNextEntityWithExtInfo()).thenReturn(mockEntityWithExtInfo);
        when(mockEntityImportStream.getPosition()).thenReturn(1);

        AtlasEntity mockEntity = mock(AtlasEntity.class);
        when(mockEntityWithExtInfo.getEntity()).thenReturn(mockEntity);
        when(mockEntity.getGuid()).thenReturn("entity-guid");

        // Act
        asyncImportTaskExecutor.publishEntityNotification(mockImportRequest, mockEntityImportStream);

        // Assert
        verify(notificationInterface).send(eq("test-topic"), anyList(), any());
        verify(mockEntityImportStream).onImportComplete("entity-guid");
        verify(importService).updateImportRequest(mockImportRequest);
        verify(mockImportRequest).setSkipTo(1);
        assertEquals(1, mockImportRequest.getImportDetails().getPublishedEntityCount());
    }

    @Test
    void testPublishEntityNotificationNullEntity() throws NotificationException {
        // Arrange
        AtlasAsyncImportRequest mockImportRequest = mock(AtlasAsyncImportRequest.class);
        EntityImportStream mockEntityImportStream = mock(EntityImportStream.class);
        when(mockImportRequest.getImportDetails()).thenReturn(new AtlasAsyncImportRequest.ImportDetails());
        when(mockEntityImportStream.hasNext()).thenReturn(true, false); // One entity in the stream
        when(mockEntityImportStream.getNextEntityWithExtInfo()).thenReturn(null);
        when(mockEntityImportStream.getPosition()).thenReturn(1);

        // Act
        asyncImportTaskExecutor.publishEntityNotification(mockImportRequest, mockEntityImportStream);

        // Assert
        verify(notificationInterface, never()).send(anyString(), anyList(), any());
        verify(mockEntityImportStream, never()).onImportComplete(anyString());
        verify(importService).updateImportRequest(mockImportRequest);
        verify(mockImportRequest).setSkipTo(1);
        assertEquals(0, mockImportRequest.getImportDetails().getPublishedEntityCount());
    }

    @Test
    void testPublishEntityNotificationExceptionInSendToTopic() throws AtlasBaseException, NotificationException {
        // Arrange
        AtlasAsyncImportRequest mockImportRequest = mock(AtlasAsyncImportRequest.class);
        AtlasImportResult mockResult = mock(AtlasImportResult.class);
        EntityImportStream mockEntityImportStream = mock(EntityImportStream.class);
        AtlasEntity.AtlasEntityWithExtInfo mockEntityWithExtInfo = mock(AtlasEntity.AtlasEntityWithExtInfo.class);
        AtlasEntity mockEntity = mock(AtlasEntity.class);
        when(mockImportRequest.getTopicName()).thenReturn("test-topic");
        when(mockImportRequest.getImportId()).thenReturn("import-id");
        when(mockImportRequest.getImportDetails()).thenReturn(new AtlasAsyncImportRequest.ImportDetails());
        when(mockImportRequest.getAtlasImportResult()).thenReturn(mockResult);
        when(mockResult.getUserName()).thenReturn("test-user-1");
        when(mockEntityImportStream.getPosition()).thenReturn(1);
        when(mockEntityImportStream.hasNext()).thenReturn(true, false);
        when(mockEntityImportStream.getNextEntityWithExtInfo()).thenReturn(mockEntityWithExtInfo);
        when(mockEntityWithExtInfo.getEntity()).thenReturn(mockEntity);
        when(mockEntity.getGuid()).thenReturn("entity-guid");

        doThrow(new NotificationException(new Exception("Error in sendToTopic")))
                .when(notificationInterface)
                .send(eq("test-topic"), anyList(), any());

        // Act
        asyncImportTaskExecutor.publishEntityNotification(mockImportRequest, mockEntityImportStream);

        // Assert
        verify(notificationInterface).send(eq("test-topic"), anyList(), any());
        verify(mockEntityImportStream, never()).onImportComplete("entity-guid");
        verify(importService).updateImportRequest(mockImportRequest);
        verify(mockImportRequest).setSkipTo(1);
        assertEquals(1, mockImportRequest.getImportDetails().getFailedEntitiesCount());
        assertEquals(0, mockImportRequest.getImportDetails().getPublishedEntityCount());
    }

    @Test
    void testPublishEntityNotificationIgnoreFailedEntityAndProcessNext() throws AtlasBaseException, NotificationException {
        // Arrange
        AtlasAsyncImportRequest mockImportRequest = mock(AtlasAsyncImportRequest.class);
        AtlasImportResult mockResult = mock(AtlasImportResult.class);
        EntityImportStream mockEntityImportStream = mock(EntityImportStream.class);
        AtlasEntity.AtlasEntityWithExtInfo mockEntityWithExtInfo = mock(AtlasEntity.AtlasEntityWithExtInfo.class);
        AtlasEntity mockEntity = mock(AtlasEntity.class);
        when(mockImportRequest.getTopicName()).thenReturn("test-topic");
        when(mockImportRequest.getImportId()).thenReturn("import-id");
        when(mockImportRequest.getImportDetails()).thenReturn(new AtlasAsyncImportRequest.ImportDetails());
        when(mockImportRequest.getAtlasImportResult()).thenReturn(mockResult);
        when(mockResult.getUserName()).thenReturn("test-user-1");
        when(mockEntityImportStream.getPosition()).thenReturn(1, 2);
        when(mockEntityImportStream.hasNext()).thenReturn(true, true, false); // Two entities
        when(mockEntityImportStream.getNextEntityWithExtInfo()).thenReturn(mockEntityWithExtInfo);
        when(mockEntityWithExtInfo.getEntity()).thenReturn(mockEntity);
        when(mockEntity.getGuid()).thenReturn("entity-guid");

        doThrow(new NotificationException(new Exception("Error in sendToTopic")))
                .doNothing()
                .when(notificationInterface)
                .send(eq("test-topic"), anyList(), any());

        // Act
        asyncImportTaskExecutor.publishEntityNotification(mockImportRequest, mockEntityImportStream);

        // Assert
        verify(notificationInterface, times(2)).send(eq("test-topic"), anyList(), any());
        verify(mockEntityImportStream, times(1)).onImportComplete("entity-guid");
        verify(importService, times(2)).updateImportRequest(mockImportRequest);
        assertEquals(1, mockImportRequest.getImportDetails().getPublishedEntityCount());
        assertEquals(1, mockImportRequest.getImportDetails().getFailedEntitiesCount());
    }

    @Test
    void testSkipToPositionHappyPath() {
        // Arrange
        AtlasAsyncImportRequest mockImportRequest = mock(AtlasAsyncImportRequest.class);
        EntityImportStream mockEntityImportStream = mock(EntityImportStream.class);
        when(mockImportRequest.getSkipTo()).thenReturn(3); // Skip to position 3
        when(mockEntityImportStream.hasNext()).thenReturn(true, true, true, true, false); // 4 entities in total
        when(mockEntityImportStream.getPosition()).thenReturn(0, 1, 2, 3);

        // Act
        asyncImportTaskExecutor.skipToPosition(mockImportRequest, mockEntityImportStream);

        // Assert
        verify(mockEntityImportStream, times(3)).next(); // Skip 3 entities
    }

    @Test
    void testSkipToPositionSkipToGreaterThanTotalEntities() {
        // Arrange
        AtlasAsyncImportRequest mockImportRequest = mock(AtlasAsyncImportRequest.class);
        EntityImportStream mockEntityImportStream = mock(EntityImportStream.class);
        when(mockImportRequest.getSkipTo()).thenReturn(10); // Skip to position 10
        when(mockEntityImportStream.hasNext()).thenReturn(true, true, true, false); // 3 entities in total
        when(mockEntityImportStream.getPosition()).thenReturn(0, 1, 2);

        // Act
        asyncImportTaskExecutor.skipToPosition(mockImportRequest, mockEntityImportStream);

        // Assert
        verify(mockEntityImportStream, times(3)).next(); // Skipped all 3 entities
    }

    @Test
    void testSkipToPositionNoEntitiesInStream() {
        // Arrange
        AtlasAsyncImportRequest mockImportRequest = mock(AtlasAsyncImportRequest.class);
        EntityImportStream mockEntityImportStream = mock(EntityImportStream.class);
        when(mockImportRequest.getSkipTo()).thenReturn(3); // Skip to position 3
        when(mockEntityImportStream.hasNext()).thenReturn(false); // No entities in the stream

        // Act
        asyncImportTaskExecutor.skipToPosition(mockImportRequest, mockEntityImportStream);

        // Assert
        verify(mockEntityImportStream, never()).next(); // No entities to skip
    }

    @Test
    void testSkipToPositionSkipToEqualsCurrentPosition() {
        // Arrange
        AtlasAsyncImportRequest mockImportRequest = mock(AtlasAsyncImportRequest.class);
        EntityImportStream mockEntityImportStream = mock(EntityImportStream.class);
        when(mockImportRequest.getSkipTo()).thenReturn(2); // Skip to position 2
        when(mockEntityImportStream.hasNext()).thenReturn(true); // At least one entity in the stream
        when(mockEntityImportStream.getPosition()).thenReturn(2); // Already at position 2

        // Act
        asyncImportTaskExecutor.skipToPosition(mockImportRequest, mockEntityImportStream);

        // Assert
        verify(mockEntityImportStream, never()).next(); // No entities skipped since current position matches skipTo
    }

    @DataProvider(name = "registerRequestScenarios")
    public Object[][] registerRequestScenarios() {
        return new Object[][] {{"null", "NEW"},            // New request
                {"SUCCESSFUL", "NEW"},      // Existing request with status SUCCESSFUL
                {"PARTIAL_SUCCESS", "NEW"}, // Existing request with status PARTIAL_SUCCESS
                {"FAILED", "NEW"},          // Existing request with status FAILED
                {"ABORTED", "NEW"},         // Existing request with status ABORTED
                {"STAGING", "RESUMED"},     // Existing request with status STAGING
                {"WAITING", "EXISTING"},    // Existing request with status WAITING
                {"PROCESSING", "EXISTING"}  // Existing request with status PROCESSING
        };
    }

    @Test(dataProvider = "registerRequestScenarios")
    public void testRegisterRequest(String existingStatus, String expectedOutcome) throws AtlasBaseException {
        // Arrange
        AtlasImportResult mockResult = mock(AtlasImportResult.class);
        AtlasAsyncImportRequest existingRequest = null;

        if (!"null".equals(existingStatus)) {
            existingRequest = mock(AtlasAsyncImportRequest.class);
            when(existingRequest.getStatus()).thenReturn(AtlasAsyncImportRequest.ImportStatus.valueOf(existingStatus));
            when(existingRequest.getImportDetails()).thenReturn(new AtlasAsyncImportRequest.ImportDetails());
        }

        when(importService.fetchImportRequestByImportId("import-id")).thenReturn(existingRequest);

        // Act
        AtlasAsyncImportRequest result = asyncImportTaskExecutor.registerRequest(mockResult, "import-id", 10, Collections.emptyList());

        // Assert
        assertNotNull(result);

        if ("NEW".equals(expectedOutcome)) {
            verify(importService).saveImportRequest(any(AtlasAsyncImportRequest.class));
        } else if ("RESUMED".equals(expectedOutcome)) {
            verify(existingRequest).setReceivedAt(any(long.class));
            verify(importService).updateImportRequest(existingRequest);
        } else if ("EXISTING".equals(expectedOutcome)) {
            verify(importService, never()).saveImportRequest(any(AtlasAsyncImportRequest.class));
            verify(importService, never()).updateImportRequest(any(AtlasAsyncImportRequest.class));
        }
    }

    @Test
    public void testRegisterRequestThrowsException() throws AtlasBaseException {
        // Arrange
        AtlasImportResult mockResult = mock(AtlasImportResult.class);
        AtlasAsyncImportRequest mockImportRequest = mock(AtlasAsyncImportRequest.class);

        when(mockImportRequest.getStatus()).thenReturn(AtlasAsyncImportRequest.ImportStatus.SUCCESSFUL);
        when(mockImportRequest.getImportDetails()).thenReturn(new AtlasAsyncImportRequest.ImportDetails());
        when(importService.fetchImportRequestByImportId("import-id")).thenReturn(mockImportRequest);
        doThrow(new AtlasBaseException("Some error while saving")).when(importService).saveImportRequest(any(AtlasAsyncImportRequest.class));

        // Act
        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> asyncImportTaskExecutor.registerRequest(mockResult, "import-id", 10, Collections.emptyList()));

        // Assert
        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.IMPORT_REGISTRATION_FAILED);
    }

    @Test
    public void testAbortImport() throws AtlasBaseException {
        // Arrange
        AtlasAsyncImportRequest mockImportRequest = mock(AtlasAsyncImportRequest.class);

        when(mockImportRequest.getTopicName()).thenReturn("ATLAS_IMPORT_12344");
        when(importService.abortImport(any(String.class))).thenReturn(mockImportRequest);
        doNothing().when(notificationInterface).deleteTopics(any(NotificationInterface.NotificationType.class), any(String.class));

        // Act
        asyncImportTaskExecutor.abortImport("12344");

        // Assert
        verify(importService, times(1)).abortImport("12344");
        verify(notificationInterface, times(1)).deleteTopics(NotificationInterface.NotificationType.ASYNC_IMPORT, "ATLAS_IMPORT_12344");
    }
}
