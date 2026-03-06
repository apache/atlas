package org.apache.atlas.repository.store.graph.v2.tags;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.Tag;
import org.apache.atlas.model.instance.AtlasClassification;

import java.util.Collection;
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

    PaginatedVertexIdResult getVertexIdFromTagsByIdTableWithPagination(String pagingStateStr, int pageSize) throws AtlasBaseException;

    /**
     * Returns only the classification type names for a vertex, without deserializing full classification JSON.
     * This is a lightweight alternative to getAllClassificationsForVertex() when only names are needed.
     *
     * @param vertexId   The vertex ID to fetch classification names for
     * @param propagated {@code null} for all, {@code true} for propagated only, {@code false} for direct only
     * @return List of classification type names (excluding deleted)
     * @throws AtlasBaseException If an error occurs during retrieval
     */
    List<String> getClassificationNamesForVertex(String vertexId, Boolean propagated) throws AtlasBaseException;

    /**
     * Batch-fetches classifications for multiple vertices in parallel.
     * Returns a map of vertex ID to its classifications list.
     * Vertices with no tags will have an empty list in the result.
     * Vertices that fail to fetch will be absent from the result (caller should fall back to sync fetch).
     *
     * @param vertexIds Collection of vertex IDs to fetch classifications for
     * @return Map of vertex ID to list of classifications
     * @throws AtlasBaseException If an error occurs during retrieval
     */
    Map<String, List<AtlasClassification>> getAllClassificationsForVertices(Collection<String> vertexIds) throws AtlasBaseException;
}

