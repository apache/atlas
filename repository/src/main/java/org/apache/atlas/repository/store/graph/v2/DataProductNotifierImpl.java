package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.listener.EntityChangeListenerV2;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.*;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.graph.GraphHelper.*;


@Component
public class DataProductNotifierImpl implements IAtlasMinimalChangeNotifier {

    private final Set<EntityChangeListenerV2> entityChangeListenersV2;

    @Inject
    public DataProductNotifierImpl(Set<EntityChangeListenerV2> entityChangeListenersV2) {
        this.entityChangeListenersV2 = entityChangeListenersV2;

    }

    @Override
    public void onEntitiesMutation(final List<AtlasVertex> vertices) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("onEntitiesMutation");
        final List<AtlasEntity> entities = new ArrayList<>(0);
        vertices.forEach(item -> entities.add(createAtlasEntity(item)));
        for (EntityChangeListenerV2 listener : entityChangeListenersV2) {
            listener.onEntitiesUpdated(entities, false);
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private AtlasEntity createAtlasEntity(AtlasVertex vertex) {
        AtlasEntity atlasEntity = new AtlasEntity();
        atlasEntity.setAttribute(QUALIFIED_NAME, vertex.getProperty(QUALIFIED_NAME, String.class));
        atlasEntity.setAttribute(NAME, vertex.getProperty(NAME, String.class));

        atlasEntity.setGuid(vertex.getProperty(GUID_PROPERTY_KEY, String.class));
        atlasEntity.setTypeName(vertex.getProperty(TYPE_NAME_PROPERTY_KEY, String.class));
        atlasEntity.setCreatedBy(vertex.getProperty(CREATED_BY_KEY, String.class));
        atlasEntity.setUpdatedBy(vertex.getProperty(MODIFIED_BY_KEY, String.class));
        atlasEntity.setCreateTime(new Date(vertex.getProperty(TIMESTAMP_PROPERTY_KEY, Long.class)));
        atlasEntity.setUpdateTime(new Date(vertex.getProperty(MODIFICATION_TIMESTAMP_PROPERTY_KEY, Long.class)));
        atlasEntity.setIsProxy(vertex.getProperty(IS_PROXY_KEY, Boolean.class));
        atlasEntity.setIsIncomplete(vertex.getProperty(IS_INCOMPLETE_PROPERTY_KEY, Boolean.class));
        atlasEntity.setStatus(getStatus(vertex));
        atlasEntity.setProvenanceType(getProvenanceType(vertex));
        atlasEntity.setHomeId(getHomeId(vertex));
        atlasEntity.setVersion(getVersion(vertex));


        return atlasEntity;
    }


}
