package org.apache.atlas.repository.store.graph.v2.preprocessor.contract;

import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.*;
import org.apache.atlas.repository.store.graph.v2.preprocessor.ConnectionPreProcessor;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.graph.AtlasGraphProvider.getGraphInstance;


public class ContractVersionUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionPreProcessor.class);

    private final EntityMutationContext context;
    public final EntityGraphRetriever entityRetriever;
    private final AtlasTypeRegistry atlasTypeRegistry;
    private AtlasEntityStore entityStore;

    private AtlasEntity entity;
    private AtlasEntity existingEntity;
    public final AtlasGraph graph;
    private List<AtlasEntity> versionList;



    public ContractVersionUtils(AtlasEntity entity, EntityMutationContext context, EntityGraphRetriever entityRetriever,
                                AtlasTypeRegistry atlasTypeRegistry, AtlasEntityStore entityStore, AtlasGraph graph) {
        this.context = context;
        this.entityRetriever = entityRetriever;
        this.atlasTypeRegistry = atlasTypeRegistry;
        this.graph = graph;
        this.entityStore = entityStore;
        this.entity = entity;
    }

    private void extractAllVersions() {
        String entityQNamePrefix = (String) entity.getAttribute(QUALIFIED_NAME);

        AtlasEntityType entityType = atlasTypeRegistry.getEntityTypeByName("DataContract");
        Integer versionCounter = 1;
        boolean found = true;

        List<AtlasEntity> versionList = new ArrayList<>();

        while (found) {
            Map<String, Object> uniqAttributes = new HashMap<>();
            uniqAttributes.put(QUALIFIED_NAME, String.format("%s/version/V%s", entityQNamePrefix, versionCounter++));
            try {
                AtlasVertex entityVertex = AtlasGraphUtilsV2.getVertexByUniqueAttributes(graph, entityType, uniqAttributes);
                AtlasEntity entity = entityRetriever.toAtlasEntity(entityVertex);

                versionList.add(entity);
            } catch (AtlasBaseException ex) {
                found = false;
            }

        }
        this.versionList = versionList;

    }

    public AtlasEntity getLatestVersion() throws AtlasBaseException {
        if (this.versionList == null) {
            extractAllVersions();
        }
        Collections.sort(this.versionList, (e1, e2) -> {
            String e1QName = (String) e1.getAttribute(QUALIFIED_NAME);
            String e2QName = (String) e2.getAttribute(QUALIFIED_NAME);

            return e2QName.compareTo(e1QName);
        });
        if (this.versionList.isEmpty()) {
            return null;
        }
        return this.versionList.get(0);
    }


    public Iterator<AtlasVertex> getAllEntityVersions() {
        String entityQNamePrefix = (String) entity.getAttribute(QUALIFIED_NAME);
        AtlasEntityType entityType = atlasTypeRegistry.getEntityTypeByName("DataContract");

//        AtlasEntityType entityType, String name
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("getAllEntityVersions");
        AtlasGraph graph                = getGraphInstance();
        AtlasGraphQuery query           = graph.query()
                .has(ENTITY_TYPE_PROPERTY_KEY, entityType.getTypeName())
                .has(STATE_PROPERTY_KEY, AtlasEntity.Status.ACTIVE.name())
                .has(entityType.getAllAttributes().get(QUALIFIED_NAME).getQualifiedName(), String.format("%s/version/V1", entityQNamePrefix));


        Iterator<AtlasVertex> result = query.vertices().iterator();

        RequestContext.get().endMetricRecord(metric);
        return result;
    }

    public void createNewVersion() throws AtlasBaseException {
        AtlasVertex vertex = context.getVertex(entity.getGuid());
        AtlasEntity existingContractEntity = entityRetriever.toAtlasEntity(vertex);
//        this.newEntity = new AtlasEntity(existingEntity);
        this.existingEntity.setAttribute(QUALIFIED_NAME, null);

        try {
            RequestContext.get().setSkipAuthorizationCheck(true);
            EntityStream entityStream = new AtlasEntityStream(existingEntity);
            entityStore.createOrUpdate(entityStream, false);
            LOG.info("Created bootstrap policies for connection {}", existingEntity.getAttribute(QUALIFIED_NAME));
        } finally {
            RequestContext.get().setSkipAuthorizationCheck(false);
        }

    }
}
