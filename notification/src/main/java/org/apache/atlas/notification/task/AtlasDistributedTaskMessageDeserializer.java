package org.apache.atlas.notification.task;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.atlas.model.notification.AtlasNotificationMessage;
import org.apache.atlas.model.notification.AtlasDistributedTaskNotification;
import org.apache.atlas.notification.AbstractMessageDeserializer;
import org.apache.atlas.notification.AbstractNotification;
import org.apache.atlas.notification.hook.HookMessageDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AtlasDistributedTaskMessageDeserializer extends AbstractMessageDeserializer<AtlasDistributedTaskNotification> {
    private static final Logger NOTIFICATION_LOGGER = LoggerFactory.getLogger(HookMessageDeserializer.class);

    public AtlasDistributedTaskMessageDeserializer() {
        super(new TypeReference<AtlasDistributedTaskNotification>() {},
              new TypeReference<AtlasNotificationMessage<AtlasDistributedTaskNotification>>() {},
              AbstractNotification.CURRENT_MESSAGE_VERSION, NOTIFICATION_LOGGER);
    }

    @Override
    public AtlasDistributedTaskNotification deserialize(String messageJson) {
        final AtlasDistributedTaskNotification ret = super.deserialize(messageJson);

        if (ret != null) {
            ret.normalize();
        }

        return ret;
    }

}
