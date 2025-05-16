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

import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.kafka.NotificationProvider;
import org.apache.atlas.model.impexp.AtlasAsyncImportRequest;
import org.apache.atlas.model.impexp.AtlasAsyncImportRequest.ImportStatus;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.model.notification.ImportNotification;
import org.apache.atlas.model.notification.MessageSource;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.notification.NotificationException;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.atlas.repository.impexp.AsyncImportService;
import org.apache.atlas.repository.store.graph.v2.asyncimport.ImportTaskListener;
import org.apache.commons.lang.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

import java.util.Collections;
import java.util.List;

import static org.apache.atlas.notification.NotificationInterface.NotificationType.ASYNC_IMPORT;

@Component
public class AsyncImportTaskExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncImportTaskExecutor.class);

    private static final String MESSAGE_SOURCE = AsyncImportTaskExecutor.class.getSimpleName();

    private final AsyncImportService    importService;
    private final NotificationInterface notificationInterface;
    private final ImportTaskListener    importTaskListener;
    private final MessageSource         messageSource;

    @Inject
    public AsyncImportTaskExecutor(AsyncImportService importService, ImportTaskListener importTaskListener) {
        this.importService         = importService;
        this.notificationInterface = NotificationProvider.get();
        this.importTaskListener    = importTaskListener;
        this.messageSource         = new MessageSource(MESSAGE_SOURCE);
    }

    public AtlasAsyncImportRequest run(AtlasImportResult result, EntityImportStream entityImportStream) throws AtlasBaseException {
        try {
            String                  importId      = entityImportStream.getMd5Hash();
            AtlasAsyncImportRequest importRequest = registerRequest(result, importId, entityImportStream.size(), entityImportStream.getCreationOrder());

            if (ObjectUtils.equals(importRequest.getStatus(), ImportStatus.WAITING) || ObjectUtils.equals(importRequest.getStatus(), ImportStatus.PROCESSING)) {
                LOG.warn("AsyncImportTaskExecutor.run(): Import request with id={} is already in state={}", importId, importRequest.getStatus());
            } else {
                // skip to the most recent published position
                if (ObjectUtils.equals(importRequest.getStatus(), ImportStatus.STAGING)) {
                    skipToStartEntityPosition(importRequest, entityImportStream);
                }

                publishImportRequest(importRequest, entityImportStream);
            }

            return importRequest;
        } catch (AtlasBaseException abe) {
            throw new AtlasBaseException(AtlasErrorCode.IMPORT_FAILED, abe);
        } finally {
            entityImportStream.close();
        }
    }

    public void publishTypeDefNotification(AtlasAsyncImportRequest importRequest, AtlasTypesDef atlasTypesDef) throws AtlasBaseException {
        LOG.info("==> publishTypeDefNotification(importRequest={}, atlasTypesDef={})", importRequest, atlasTypesDef);

        try {
            HookNotification typeDefImportNotification = new ImportNotification.AtlasTypesDefImportNotification(importRequest.getImportId(), importRequest.getImportResult().getUserName(), atlasTypesDef);

            sendToTopic(importRequest.getTopicName(), typeDefImportNotification);
        } finally {
            LOG.info("<== publishTypeDefNotification(atlasAsyncImportRequest={})", importRequest);
        }
    }

    public void onCompleteImportRequest(String importId) {
        importTaskListener.onCompleteImportRequest(importId);
    }

    public void abortAsyncImportRequest(String importId) throws AtlasBaseException {
        LOG.info("==> abortAsyncImportRequest(importId={})", importId);

        try {
            AtlasAsyncImportRequest importRequest = importService.abortImport(importId);

            notificationInterface.deleteTopic(ASYNC_IMPORT, importRequest.getTopicName());
        } catch (AtlasBaseException abe) {
            throw new AtlasBaseException(AtlasErrorCode.ABORT_IMPORT_FAILED, abe, importId);
        } finally {
            LOG.info("<== abortAsyncImportRequest(importId={})", importId);
        }
    }

    public void delete() {
        LOG.info("==> delete()");

        importService.deleteRequests();

        LOG.info("<== delete()");
    }

    @VisibleForTesting
    void publishImportRequest(AtlasAsyncImportRequest importRequest, EntityImportStream entityImportStream) throws AtlasBaseException {
        try {
            LOG.info("==> publishImportRequest(atlasAsyncImportRequest={})", importRequest);

            publishTypeDefNotification(importRequest, entityImportStream.getTypesDef());
            publishEntityNotification(importRequest, entityImportStream);

            importRequest.setStagedTime(System.currentTimeMillis());

            importService.updateImportRequest(importRequest);

            importTaskListener.onReceiveImportRequest(importRequest);
        } finally {
            notificationInterface.closeProducer(ASYNC_IMPORT, importRequest.getTopicName());

            LOG.info("<== publishImportRequest(atlasAsyncImportRequest={})", importRequest);
        }
    }

    @VisibleForTesting
    void publishEntityNotification(AtlasAsyncImportRequest importRequest, EntityImportStream entityImportStream) {
        LOG.info("==> publishEntityNotification(atlasAsyncImportRequest={})", importRequest);

        int publishedEntityCounter = importRequest.getImportDetails().getPublishedEntityCount();
        int failedEntityCounter    = importRequest.getImportDetails().getFailedEntitiesCount();

        while (entityImportStream.hasNext()) {
            AtlasEntityWithExtInfo entityWithExtInfo   = entityImportStream.getNextEntityWithExtInfo();
            AtlasEntity            entity              = entityWithExtInfo != null ? entityWithExtInfo.getEntity() : null;
            int                    startEntityPosition = entityImportStream.getPosition();

            try {
                if (entity == null) {
                    continue;
                }

                HookNotification entityImportNotification = new ImportNotification.AtlasEntityImportNotification(importRequest.getImportId(), importRequest.getImportResult().getUserName(), entityWithExtInfo, entityImportStream.getPosition());

                sendToTopic(importRequest.getTopicName(), entityImportNotification);

                entityImportStream.onImportComplete(entity.getGuid());

                publishedEntityCounter += 1;
            } catch (AtlasBaseException abe) {
                failedEntityCounter += 1;

                LOG.warn("AsyncImport(id={}): failed to publish entity guid={}", importRequest.getImportId(), entity.getGuid(), abe);

                importRequest.getImportDetails().getFailedEntities().add(entity.getGuid());
                importRequest.getImportDetails().setFailedEntitiesCount(failedEntityCounter);
                importRequest.getImportDetails().getFailures().put(entity.getGuid(), abe.getMessage());
            } finally {
                importRequest.getImportTrackingInfo().setStartEntityPosition(startEntityPosition);
                importRequest.getImportDetails().setPublishedEntityCount(publishedEntityCounter);

                importService.updateImportRequest(importRequest);

                LOG.info("<== publishEntityNotification(atlasAsyncImportRequest={})", importRequest);
            }
        }
    }

    @VisibleForTesting
    void skipToStartEntityPosition(AtlasAsyncImportRequest importRequest, EntityImportStream entityImportStream) {
        int startEntityPosition = importRequest.getImportTrackingInfo().getStartEntityPosition();

        LOG.info("==> skipToStartEntityPosition(atlasAsyncImportRequest={}): position={}", importRequest, startEntityPosition);

        while (entityImportStream.hasNext() && startEntityPosition > entityImportStream.getPosition()) {
            entityImportStream.next();
        }

        LOG.info("<== skipToStartEntityPosition(atlasAsyncImportRequest={}): position={}", importRequest, startEntityPosition);
    }

    @VisibleForTesting
    AtlasAsyncImportRequest registerRequest(AtlasImportResult result, String importId, int totalEntities, List<String> creationOrder) throws AtlasBaseException {
        LOG.info("==> registerRequest(importId={})", importId);

        try {
            AtlasAsyncImportRequest existingImportRequest = importService.fetchImportRequestByImportId(importId);

            // handle new , successful and failed request from scratch
            if (existingImportRequest == null
                    || ObjectUtils.equals(existingImportRequest.getStatus(), ImportStatus.SUCCESSFUL)
                    || ObjectUtils.equals(existingImportRequest.getStatus(), ImportStatus.PARTIAL_SUCCESS)
                    || ObjectUtils.equals(existingImportRequest.getStatus(), ImportStatus.FAILED)
                    || ObjectUtils.equals(existingImportRequest.getStatus(), ImportStatus.ABORTED)) {
                AtlasAsyncImportRequest newImportRequest = new AtlasAsyncImportRequest(result);

                newImportRequest.setImportId(importId);
                newImportRequest.setReceivedTime(System.currentTimeMillis());
                newImportRequest.getImportDetails().setTotalEntitiesCount(totalEntities);
                newImportRequest.getImportDetails().setCreationOrder(creationOrder);

                importService.saveImportRequest(newImportRequest);

                LOG.info("registerRequest(importId={}): registered new request {}", importId, newImportRequest);

                return newImportRequest;
            } else if (ObjectUtils.equals(existingImportRequest.getStatus(), ImportStatus.STAGING)) {
                // if we are resuming staging, we need to update the latest request received at
                existingImportRequest.setReceivedTime(System.currentTimeMillis());

                importService.updateImportRequest(existingImportRequest);
            }

            // handle request in STAGING / WAITING / PROCESSING status as resume
            LOG.info("registerRequest(importId={}): not a new request, resuming {}", importId, existingImportRequest);

            return existingImportRequest;
        } catch (AtlasBaseException abe) {
            LOG.error("Failed to register import request id={}", importId, abe);

            throw new AtlasBaseException(AtlasErrorCode.IMPORT_REGISTRATION_FAILED, abe);
        } finally {
            LOG.info("<== registerRequest(importId={})", importId);
        }
    }

    private void sendToTopic(String topic, HookNotification notification) throws AtlasBaseException {
        try {
            notificationInterface.send(topic, Collections.singletonList(notification), messageSource);
        } catch (NotificationException exp) {
            throw new AtlasBaseException(exp);
        }
    }
}
