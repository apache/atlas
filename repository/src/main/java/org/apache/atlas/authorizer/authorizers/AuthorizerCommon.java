package org.apache.atlas.authorizer.authorizers;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class AuthorizerCommon {
    private static final Logger LOG = LoggerFactory.getLogger(AuthorizerCommon.class);

    private static AtlasTypeRegistry typeRegistry;
    private static EntityGraphRetriever entityRetriever;

    @Inject
    public AuthorizerCommon(AtlasGraph graph, AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
        this.entityRetriever = new EntityGraphRetriever(graph, typeRegistry, true);
    }

    public static String getCurrentUserName() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();

        return auth != null ? auth.getName() : "";
    }

    public static boolean arrayListContains(List<String> listA, List<String> listB) {
        for (String listAItem : listA){
            if (listB.contains(listAItem)) {
                return true;
            }
        }
        return false;
    }

    public static Map<String, Object> getMap(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    public static AtlasEntityType getEntityTypeByName(String typeName) {
        return typeRegistry.getEntityTypeByName(typeName);
    }

    public static AtlasEntity toAtlasEntityHeaderWithClassifications(String guid) throws AtlasBaseException {
        //return new AtlasEntity(entityRetriever.toAtlasEntityHeaderWithClassifications(guid));
        return new AtlasEntity(entityRetriever.toAtlasEntity(guid));
    }

    public static AtlasEntity toAtlasEntityHeaderWithClassifications(AtlasVertex vertex) throws AtlasBaseException {
        //return new AtlasEntity(entityRetriever.toAtlasEntityHeaderWithClassifications(vertex));
        return new AtlasEntity(entityRetriever.toAtlasEntity(vertex));
    }
}
