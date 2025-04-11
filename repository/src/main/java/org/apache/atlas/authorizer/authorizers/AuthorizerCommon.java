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

    public static boolean listStartsWith(String value, List<String> list) {
        for (String item : list){
            if (item.startsWith(value)) {
                return true;
            }
        }
        return false;
    }

    public static boolean listMatchesWith(String value, List<String> list) {
        for (String item : list){
            if (item.matches(value.replace("*", ".*"))) {
                return true;
            }
        }
        return false;
    }

    public static boolean listEndsWith(String value, List<String> list) {
        for (String item : list){
            if (item.endsWith(value)) {
                return true;
            }
        }
        return false;
    }

    public static Set<String> getTypeAndSupertypesList(String typeName) {
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);

        if (entityType == null) {
            return Collections.singleton(typeName);
        } else {
            return entityType.getTypeAndAllSuperTypes();
        }
    }

    public static AtlasEntityType getEntityTypeByName(String typeName) {
        return typeRegistry.getEntityTypeByName(typeName);
    }

    public static AtlasEntityHeader toAtlasEntityHeaderWithClassifications(String guid) throws AtlasBaseException {
        //return new AtlasEntity(entityRetriever.toAtlasEntityHeaderWithClassifications(guid));
        return entityRetriever.toAtlasEntityHeaderWithClassifications(guid);
    }

    public static AtlasEntityHeader toAtlasEntityHeaderWithClassifications(AtlasVertex vertex) throws AtlasBaseException {
        //return new AtlasEntity(entityRetriever.toAtlasEntityHeaderWithClassifications(vertex));
        return entityRetriever.toAtlasEntityHeaderWithClassifications(vertex);
    }

    public static boolean isResourceMatch(List<String> policyValues, String actualValue) {
        return isResourceMatch(policyValues, actualValue, false);
    }

    public static boolean isResourceMatch(List<String> policyValues, String actualValue, boolean replaceUser) {
        if (!policyValues.contains("*")) {
            if (replaceUser) {
                return policyValues.stream().anyMatch(x -> actualValue.matches(x
                        .replace("{USER}", AuthorizerCommon.getCurrentUserName())
                        .replace("*", ".*")));
            } else {
                return policyValues.stream().anyMatch(x -> actualValue.matches(x.replace("*", ".*")));
            }
        }
        return true;
    }

    public static boolean isResourceMatch(List<String> policyValues, Set<String> entityValues) {
        if (!policyValues.contains("*")) {
            return entityValues.stream().anyMatch(assetType -> policyValues.stream().anyMatch(policyAssetType -> assetType.matches(policyAssetType.replace("*", ".*"))));
        }
        return true;
    }

    public static boolean isTagResourceMatch(List<String> policyValues, AtlasEntityHeader entityHeader) {
        if (!policyValues.contains(("*"))) {
            if (entityHeader.getClassifications() == null || entityHeader.getClassifications().isEmpty()) {
                //since entity does not have tags at all, it should not pass this evaluation
                return false;
            }

            List<String> assetTags = entityHeader.getClassifications().stream().map(x -> x.getTypeName()).collect(Collectors.toList());

            return assetTags.stream().anyMatch(assetTag -> policyValues.stream().anyMatch(policyAssetType -> assetTag.matches(policyAssetType.replace("*", ".*"))));
        }
        return true;
    }
}
