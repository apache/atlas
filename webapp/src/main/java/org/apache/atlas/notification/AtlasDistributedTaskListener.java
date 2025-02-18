package org.apache.atlas.notification;

import org.apache.atlas.model.notification.AtlasDistributedTaskNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AtlasDistributedTaskListener {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasDistributedTaskListener.class);

    private final NotificationInterface notificationInterface;

    @Inject
    public AtlasDistributedTaskListener(NotificationInterface notificationInterface) {
        this.notificationInterface = notificationInterface;
    }

    public void createRelationshipCleanTask(String vertexId, List<String> edgeLabels, Long batchSize) {
        Map<String, Object> taskParams = new HashMap<>();
        taskParams.put("vertexId", vertexId);
        taskParams.put("edgeLabels", edgeLabels);
        taskParams.put("batchSize", batchSize);
        AtlasDistributedTaskNotification notification = new AtlasDistributedTaskNotification(AtlasDistributedTaskNotification.AtlasTaskType.CLEANUP_ARCHIVED_RELATIONSHIPS, taskParams);

        try {
            notificationInterface.send(NotificationInterface.NotificationType.ATLAS_TASKS, notification);
        } catch (NotificationException e) {
            LOG.error("Failed to send notification for task: {}", notification, e);
        }
    }
}
