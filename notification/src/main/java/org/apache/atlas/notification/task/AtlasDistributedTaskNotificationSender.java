package org.apache.atlas.notification.task;

import org.apache.atlas.model.notification.AtlasDistributedTaskNotification;
import org.apache.atlas.notification.NotificationException;
import org.apache.atlas.notification.NotificationInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class AtlasDistributedTaskNotificationSender {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasDistributedTaskNotificationSender.class);

    private final NotificationInterface notificationInterface;

    private final static Long batchSize = 40000L;

    @Autowired
    public AtlasDistributedTaskNotificationSender(NotificationInterface notificationInterface) {
        this.notificationInterface = notificationInterface;
    }

    public AtlasDistributedTaskNotification createRelationshipCleanUpTask(String vertexId, List<String> edgeLabels) {
        Map<String, Object> taskParams = new HashMap<>();
        taskParams.put("vertexId", vertexId);
        taskParams.put("edgeLabels", edgeLabels);
        taskParams.put("batchSize", batchSize);
        AtlasDistributedTaskNotification notification = new AtlasDistributedTaskNotification(AtlasDistributedTaskNotification.AtlasTaskType.CLEANUP_ARCHIVED_RELATIONSHIPS, taskParams);

       return notification;
    }

    public AtlasDistributedTaskNotification createHasLineageCalculationTasks(Map<String, String> typeByVertexId) {
        Map<String, Object> taskParams = new HashMap<>();
        taskParams.put("typeByVertexId", typeByVertexId);
        AtlasDistributedTaskNotification notification = new AtlasDistributedTaskNotification(AtlasDistributedTaskNotification.AtlasTaskType.CALCULATE_HAS_LINEAGE, taskParams);

        return  notification;
    }

    public void send(AtlasDistributedTaskNotification notification) {
        try {
            notificationInterface.send(NotificationInterface.NotificationType.ATLAS_DISTRIBUTED_TASKS, notification);
        } catch (NotificationException e) {
            LOG.error("Failed to send notification for task: {}", notification, e);
        }
    }

    public void send(List<AtlasDistributedTaskNotification> notifications) {
        try {
            notificationInterface.send(NotificationInterface.NotificationType.ATLAS_DISTRIBUTED_TASKS, notifications);
        } catch (NotificationException e) {
            LOG.error("Failed to send notifications for tasks: {}", notifications, e);
        }
    }
}
