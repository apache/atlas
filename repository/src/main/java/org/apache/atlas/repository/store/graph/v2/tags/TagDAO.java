package org.apache.atlas.repository.store.graph.v2.tags;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.Tag;
import org.apache.atlas.model.instance.AtlasClassification;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface TagDAO {

    List<AtlasClassification> getAllDirectClassificationsForVertex(String vertexId) throws AtlasBaseException;

    List<AtlasClassification> getAllClassificationsForVertex(String vertexId) throws AtlasBaseException;
    List<Tag> getAllTagsByVertexId(String vertexId) throws AtlasBaseException;

    AtlasClassification findDirectDeletedTagByVertexIdAndTagTypeName(String vertexId, String tagTypeName) throws AtlasBaseException;

    Tag findDirectTagByVertexIdAndTagTypeNameWithAssetMetadata(String vertexId, String tagTypeName, boolean includeDeleted) throws AtlasBaseException;
    PaginatedTagResult getPropagationsForAttachmentBatch(String sourceVertexId, String tagTypeName, String storedPagingState) throws AtlasBaseException;

    List<AtlasClassification> findByVertexIdAndPropagated(String vertexId) throws AtlasBaseException;

    AtlasClassification findDirectTagByVertexIdAndTagTypeName(String assetVertexId, String tagTypeName, boolean includeDeleted) throws AtlasBaseException;

    void putPropagatedTags(String sourceAssetId,
                           String tagTypeName,
                           Set<String> propagatedAssetVertexIds,
                           Map<String, Map<String, Object>> assetMinAttrsMap,
                           AtlasClassification tag) throws AtlasBaseException;

    void putDirectTag(String assetId, String tagTypeName,
                      AtlasClassification tag,
                      Map<String, Object> assetMetadata) throws AtlasBaseException;

    void deleteDirectTag(String sourceVertexId, AtlasClassification tagToDelete) throws AtlasBaseException;
    void deleteTags(List<Tag> tagsToDelete) throws AtlasBaseException;

    List<AtlasClassification> getPropagationsForAttachment(String vertexId, String sourceEntityGuid) throws AtlasBaseException;

    /**
     * Retrieves a paginated list of propagated tags for a given source vertex and tag type.
     * 
     * @param sourceVertexId The source vertex ID
     * @param tagTypeName The tag type name
     * @param pagingStateStr The paging state from a previous query (null for first page)
     * @param pageSize The number of results to return per page
     * @return A PaginatedTagResult containing the tags and pagination state
     * @throws AtlasBaseException If an error occurs during retrieval
     */
    PaginatedTagResult getPropagationsForAttachmentBatchWithPagination(String sourceVertexId, String tagTypeName,
                                                                       String pagingStateStr, int pageSize) throws AtlasBaseException;
}

