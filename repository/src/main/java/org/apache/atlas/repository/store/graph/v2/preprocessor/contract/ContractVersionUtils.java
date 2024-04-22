package org.apache.atlas.repository.store.graph.v2.preprocessor.contract;

import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.*;
import org.apache.atlas.type.AtlasTypeRegistry;

import java.util.*;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;


public class ContractVersionUtils {

    private final EntityMutationContext context;
    public final EntityGraphRetriever entityRetriever;
    private final AtlasTypeRegistry atlasTypeRegistry;
    private AtlasEntityStore entityStore;

    private AtlasEntity entity;

    public final AtlasGraph graph;
    private List<AtlasEntityHeader> versionList;
    private EntityDiscoveryService discovery;

    public ContractVersionUtils(AtlasEntity entity, EntityMutationContext context, EntityGraphRetriever entityRetriever,
                                AtlasTypeRegistry atlasTypeRegistry, AtlasEntityStore entityStore, AtlasGraph graph,
                                EntityDiscoveryService discovery) {
        this.context = context;
        this.entityRetriever = entityRetriever;
        this.atlasTypeRegistry = atlasTypeRegistry;
        this.graph = graph;
        this.entityStore = entityStore;
        this.entity = entity;
        this.discovery = discovery;
    }

    private void extractAllVersions() throws AtlasBaseException {
        String contractQName = (String) entity.getAttribute(QUALIFIED_NAME);
        String datasetQName = contractQName.substring(0, contractQName.lastIndexOf("/contract"));
        List<AtlasEntityHeader> ret = new ArrayList<>();

        IndexSearchParams indexSearchParams = new IndexSearchParams();
        Map<String, Object> dsl = new HashMap<>();

        List mustClauseList = new ArrayList();
        mustClauseList.add(mapOf("term", mapOf("__typeName.keyword", CONTRACT_ENTITY_TYPE)));
        mustClauseList.add(mapOf("wildcard", mapOf(QUALIFIED_NAME, String.format("%s/contract/version/*", datasetQName))));

        dsl.put("query", mapOf("bool", mapOf("must", mustClauseList)));

        indexSearchParams.setDsl(dsl);
        indexSearchParams.setSuppressLogs(true);

        AtlasSearchResult result = discovery.directIndexSearch(indexSearchParams);
        if (result != null) {
            ret = result.getEntities();
        }
        this.versionList = ret;
    }

    public AtlasEntity getCurrentVersion() throws AtlasBaseException {
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
        return new AtlasEntity(this.versionList.get(0));
    }
}
