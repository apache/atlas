import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.notification.HookNotification.EntityPartialUpdateRequestV2;
import org.apache.atlas.notification.AbstractNotification;

import java.util.Collections;

/** Prints one ATLAS_HOOK payload with unknown typename trino_table (ATLAS-5423 E2E). */
public class GenInvalidHookMessage {
    public static void main(String[] args) {
        AtlasEntity entity = new AtlasEntity("trino_table");
        entity.setAttribute("qualifiedName", "e2e-atlas-5423-invalid-typename");

        AtlasObjectId objectId = new AtlasObjectId("trino_table",
                Collections.singletonMap("qualifiedName", "e2e-atlas-5423-invalid-typename"));

        EntityPartialUpdateRequestV2 request = new EntityPartialUpdateRequestV2("admin", objectId,
                new AtlasEntity.AtlasEntityWithExtInfo(entity));

        System.out.print(AbstractNotification.getMessageJson(request));
    }
}
