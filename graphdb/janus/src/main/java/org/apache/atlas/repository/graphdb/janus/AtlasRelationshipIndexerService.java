package org.apache.atlas.repository.graphdb.janus;

import com.google.common.collect.ImmutableMap;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.elasticsearch.action.update.UpdateRequest;
import org.janusgraph.util.encoding.LongEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.graphdb.janus.AtlasNestedRelationshipsESQueryBuilder.*;
import static org.apache.atlas.repository.graphdb.janus.AtlasRelationshipConstants.*;


@Service
public class AtlasRelationshipIndexerService implements AtlasRelationshipsService {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasRelationshipIndexerService.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("AtlasRelationshipIndexerService");
    private final AtlasJanusVertexIndexRepository atlasJanusVertexIndexRepository;
    private final AtlasTypeRegistry typeRegistry;
    private boolean isBidirectionalMappingSupported = true;

    @Inject
    public AtlasRelationshipIndexerService(AtlasJanusVertexIndexRepository atlasJanusVertexIndexRepository, AtlasTypeRegistry typeRegistry) {
        this.atlasJanusVertexIndexRepository = atlasJanusVertexIndexRepository;
        this.typeRegistry = typeRegistry;
        try {
            isBidirectionalMappingSupported = ApplicationProperties.get().getBoolean(IS_BI_DIRECTIONAL_RELATIONSHIP_MAPPING, true);
        } catch (AtlasException e) {
            LOG.error("Could not load configuration. Setting to default value for " + isBidirectionalMappingSupported, e);
        }
    }

    @Override
    public void createRelationships(List<AtlasRelationship> relationships, Map<AtlasObjectId, Object> relationshipEndToVertexIdMap) {
        if (CollectionUtils.isEmpty(relationships) || MapUtils.isEmpty(relationshipEndToVertexIdMap))
            return;
        AtlasPerfTracer perf = null;
        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG))
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "createRelationships()");
        relationships = relationships.stream().filter(r -> RELATIONSHIPS_TYPES_SUPPORTED.contains(r.getTypeName())).collect(Collectors.toList());
        try {
            if (LOG.isDebugEnabled())
                LOG.debug("==> createRelationships()");

            Map<DocIdKey, List<AtlasRelationship>> vertexDocIdToRelationshipsMap = buildDocIdToRelationshipsMap(relationships, relationshipEndToVertexIdMap);
            bulkUpdateRelationships(vertexDocIdToRelationshipsMap);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @Override
    public void deleteRelationship(AtlasRelationship relationship, Map<AtlasObjectId, Object> end1ToVertexIdMap) {
        Objects.requireNonNull(relationship, "relationship");
        AtlasPerfTracer perf = null;
        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "deleteRelationships()");
        }
        try {
            // TODO: re write deletion logic
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    private void bulkUpdateRelationships(Map<DocIdKey, List<AtlasRelationship>> vertexDocIdToRelationshipsMap) {
        List<UpdateRequest> updateRequests = new ArrayList<>();
        for (DocIdKey docIdKey : vertexDocIdToRelationshipsMap.keySet()) {
            UpdateRequest updateRequest = getQueryForAppendingNestedRelationships(docIdKey.docId, getScriptParamsMap(vertexDocIdToRelationshipsMap, docIdKey));
            updateRequests.add(updateRequest);
        }
        atlasJanusVertexIndexRepository.performBulkAsyncV2(updateRequests);
    }

    private Map<DocIdKey, List<AtlasRelationship>> buildDocIdToRelationshipsMap(List<AtlasRelationship> relationships, Map<AtlasObjectId, Object> endEntityToVertexIdMap) {
        Map<DocIdKey, List<AtlasRelationship>> docIdToRelationshipsMap = new HashMap<>();
        for (AtlasRelationship r : relationships) {
            final AtlasRelationshipDef relationshipDef = this.typeRegistry.getRelationshipDefByName(r.getTypeName());
            final AtlasObjectId end1 = r.getEnd1();
            final AtlasObjectId end2 = r.getEnd2();

            if (isBidirectionalMappingSupported)
                addBothEnds(endEntityToVertexIdMap, docIdToRelationshipsMap, r, end1, end2);
            else {
                if (relationshipDef.getEndDef1().getCardinality() == AtlasStructDef.AtlasAttributeDef.Cardinality.SET || relationshipDef.getEndDef1().getCardinality() == AtlasStructDef.AtlasAttributeDef.Cardinality.LIST)
                    addEndForRelationsMapping(endEntityToVertexIdMap, docIdToRelationshipsMap, r, end1, true);
                if (relationshipDef.getEndDef2().getCardinality() == AtlasStructDef.AtlasAttributeDef.Cardinality.SET || relationshipDef.getEndDef2().getCardinality() == AtlasStructDef.AtlasAttributeDef.Cardinality.LIST)
                    addEndForRelationsMapping(endEntityToVertexIdMap, docIdToRelationshipsMap, r, end2, false);
                if (relationshipDef.getEndDef1().getCardinality() == AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE && relationshipDef.getEndDef2().getCardinality() == AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE) {
                    if (relationshipDef.getEndDef1().getIsContainer())
                        addEndForRelationsMapping(endEntityToVertexIdMap, docIdToRelationshipsMap, r, end1, true);
                    else if (relationshipDef.getEndDef2().getIsContainer())
                        addEndForRelationsMapping(endEntityToVertexIdMap, docIdToRelationshipsMap, r, end2, false);
                    else
                        addBothEnds(endEntityToVertexIdMap, docIdToRelationshipsMap, r, end1, end2);
                }
            }
        }
        return docIdToRelationshipsMap;
    }

    private void addBothEnds(Map<AtlasObjectId, Object> endEntityToVertexIdMap, Map<DocIdKey, List<AtlasRelationship>> docIdToRelationshipsMap, AtlasRelationship r, AtlasObjectId end1, AtlasObjectId end2) {
        addEndForRelationsMapping(endEntityToVertexIdMap, docIdToRelationshipsMap, r, end1, true);
        addEndForRelationsMapping(endEntityToVertexIdMap, docIdToRelationshipsMap, r, end2, false);
    }

    private String encodeVertexIdToESDocId(Object vertex) {
        Objects.requireNonNull(vertex);
        return LongEncoding.encode(Long.parseLong(vertex.toString()));
    }

    private void addEndForRelationsMapping(Map<AtlasObjectId, Object> endEntityToVertexIdMap, Map<DocIdKey, List<AtlasRelationship>> docIdToRelationshipsMap, AtlasRelationship r, AtlasObjectId end, boolean isEnd1) {
        final String endDocId = encodeVertexIdToESDocId(endEntityToVertexIdMap.get(end));
        final DocIdKey docIdKey = new DocIdKey(endDocId, isEnd1);
        List<AtlasRelationship> existingRelationshipsForDocId1 = docIdToRelationshipsMap.getOrDefault(docIdKey, new ArrayList<>());
        existingRelationshipsForDocId1.add(r);
        docIdToRelationshipsMap.put(docIdKey, existingRelationshipsForDocId1);
    }

    private Map<String, Object> getScriptParamsMap(Map<DocIdKey, List<AtlasRelationship>> endDocIdRelationshipsMap, DocIdKey docIdKey) {
        final Map<String, Object> paramsMap = new HashMap<>();
        List<Map<String, String>> relationshipNestedPayloadList = buildParamsListForScript(endDocIdRelationshipsMap, docIdKey);
        paramsMap.put(RELATIONSHIPS_PARAMS_KEY, relationshipNestedPayloadList);
        return paramsMap;
    }

    private List<Map<String, String>> buildParamsListForScript(Map<DocIdKey, List<AtlasRelationship>> end1DocIdRelationshipsMap, DocIdKey docIdKey) {
        List<Map<String, String>> relationshipNestedPayloadList = new ArrayList<>();
        for (AtlasRelationship r : end1DocIdRelationshipsMap.get(docIdKey)) {
            Map<String, String> relationshipNestedPayload = buildNestedRelationshipDoc(r, docIdKey);
            relationshipNestedPayloadList.add(relationshipNestedPayload);
        }
        return relationshipNestedPayloadList;
    }

    private Map<String, String> buildNestedRelationshipDoc(AtlasRelationship r, DocIdKey docIdKey) {
        final AtlasRelationshipDef relationshipDef = this.typeRegistry.getRelationshipDefByName(r.getTypeName());
        String endGuid;
        String endTypeName;
        String endDefName;
        if (docIdKey.isEnd1) {
            endGuid = r.getEnd2().getGuid();
            endTypeName = r.getEnd2().getTypeName();
            endDefName = relationshipDef.getEndDef1().getName();
        } else {
            endGuid = r.getEnd1().getGuid();
            endTypeName = r.getEnd1().getTypeName();
            endDefName = relationshipDef.getEndDef2().getName();
        }
        return ImmutableMap.of(RELATIONSHIP_GUID_KEY, r.getGuid(), RELATIONSHIPS_TYPENAME_KEY, r.getTypeName(), GUID_KEY, endGuid, OPPOSITE_END_TYPENAME, endTypeName, END_DEF_NAME, endDefName);
    }

    static final class DocIdKey{
        private final String docId;
        private final boolean isEnd1;

        public DocIdKey(String docId, boolean isEnd1) {
            this.docId = docId;
            this.isEnd1 = isEnd1;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DocIdKey docIdKey = (DocIdKey) o;
            return isEnd1 == docIdKey.isEnd1 && Objects.equals(docId, docIdKey.docId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(docId, isEnd1);
        }
    }
}