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
package org.apache.atlas.notification.task;

import org.apache.atlas.model.notification.TaskNotification;
import org.apache.atlas.notification.NotificationException;
import org.apache.atlas.notification.NotificationInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class TaskNotificationSender {
    private static final Logger LOG = LoggerFactory.getLogger(TaskNotificationSender.class);

    private final NotificationInterface notificationInterface;

    @Autowired
    public TaskNotificationSender(NotificationInterface notificationInterface) {
        this.notificationInterface = notificationInterface;
    }

    public void sendTaskEvent(TaskNotification notification) {
        try {
            notificationInterface.send(NotificationInterface.NotificationType.TASK_EVENTS, notification);

            LOG.info("Sent task notification: taskId={}, taskType={}, status={}",
                    notification.getTaskId(), notification.getTaskType(), notification.getStatus());
        } catch (NotificationException e) {
            LOG.error("Failed to send task notification: taskId={}, taskType={}, status={}",
                    notification.getTaskId(), notification.getTaskType(), notification.getStatus(), e);
        }
    }
}
