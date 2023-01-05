package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;

import java.util.List;
import java.util.Map;

public interface AtlasRelationshipsService {
    void createRelationships(List<AtlasRelationship> relationships, Map<AtlasObjectId, Object> end1ToVertexIdMap) throws AtlasBaseException;
    void deleteRelationship(AtlasRelationship relationship, Map<AtlasObjectId, Object> end1ToVertexIdMap) throws AtlasBaseException;
}