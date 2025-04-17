package org.apache.atlas.repository.store.graph.v2.tags;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.Tag;
import org.apache.atlas.model.instance.AtlasClassification;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface TagDAO {

    List<AtlasClassification> getAllDirectTagsForVertex(String vertexId) throws AtlasBaseException;
    List<AtlasClassification> getTagsForVertex(String vertexId) throws AtlasBaseException;

    List<Tag> getPropagationsForAttachmentBatch(String sourceVertexId, String tagTypeName) throws AtlasBaseException;
    AtlasClassification findDirectTagByVertexIdAndTagTypeName(String assetVertexId, String tagTypeName) throws AtlasBaseException;

    void putPropagatedTags(String sourceAssetId,
                           String tagTypeName,
                           Set<String> propagatedAssetVertexIds,
                           Map<String, Map<String, Object>> assetMinAttrsMap,
                           AtlasClassification tag);

    void putDirectTag(String assetId, String tagTypeName,
                      AtlasClassification tag,
                      Map<String, Object> assetMetadata);

    void deleteDirectTag(String sourceVertexId, AtlasClassification tagToDelete) throws AtlasBaseException;
    void deleteTags(List<Tag> tagsToDelete) throws AtlasBaseException;
}