package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.listener.EntityChangeListenerV2;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.graph.GraphHelper.*;


@Component
public class AlternateLinkingNotifierImpl implements IAtlasAlternateChangeNotifier {

    private final Set<EntityChangeListenerV2> entityChangeListenersV2;

    @Inject
    public AlternateLinkingNotifierImpl(Set<EntityChangeListenerV2> entityChangeListenersV2) {
        this.entityChangeListenersV2 = entityChangeListenersV2;

    }

    @Override
    public void onEntitiesMutation(final EntityMutationResponse entityMutationResponse, final Map<String, AtlasVertex> entityByGuid) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("onEntitiesMutation");
        final List<AtlasEntityHeader> updatedEntities = entityMutationResponse.getUpdatedEntities();
        final List<AtlasEntity> entities = updatedEntities.stream().map(entityHeader -> createAtlasEntity(entityHeader, entityByGuid.get(entityHeader.getGuid()))).collect(Collectors.toList());

        for (EntityChangeListenerV2 listener : entityChangeListenersV2) {
            listener.onEntitiesUpdated(entities, false);
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private AtlasEntity createAtlasEntity(AtlasEntityHeader entityHeader, AtlasVertex vertex) {
        AtlasEntity atlasEntity = new AtlasEntity();
        atlasEntity.setAttributes(entityHeader.getAttributes());
        atlasEntity.setGuid(entityHeader.getGuid());
        atlasEntity.setTypeName(entityHeader.getTypeName());
        atlasEntity.setStatus(entityHeader.getStatus());
        atlasEntity.setCreatedBy(entityHeader.getCreatedBy());
        atlasEntity.setUpdatedBy(entityHeader.getUpdatedBy());
        atlasEntity.setCreateTime(entityHeader.getCreateTime());
        atlasEntity.setUpdateTime(entityHeader.getUpdateTime());
        atlasEntity.setIsProxy(entityHeader.getIsIncomplete());
        atlasEntity.setIsIncomplete(entityHeader.getIsIncomplete());
        atlasEntity.setProvenanceType(getProvenanceType(vertex));
        atlasEntity.setCustomAttributes(getCustomAttributes(vertex));
        atlasEntity.setHomeId(getHomeId(vertex));
        atlasEntity.setVersion(getVersion(vertex));

        return atlasEntity;
    }


}