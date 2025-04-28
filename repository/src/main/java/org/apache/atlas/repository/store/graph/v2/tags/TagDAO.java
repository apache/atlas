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

    Tag findDirectTagByVertexIdAndTagTypeNameWithAssetMetadata(String vertexId, String tagTypeName) throws AtlasBaseException;

    List<Tag> getPropagationsForAttachmentBatch(String sourceVertexId, String tagTypeName) throws AtlasBaseException;
    List<Tag> getTagPropagationsForAttachment(String sourceVertexId, String tagTypeName) throws AtlasBaseException;

    List<AtlasClassification> getPropagatedTagsForVertex(String vertexId) throws AtlasBaseException;

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
    PaginatedTagResult getPropagationsForAttachmentBatchWithPagination(String sourceVertexId, 
                                                                      String tagTypeName,
                                                                      String pagingStateStr, 
                                                                      int pageSize) throws AtlasBaseException;
}

/**
 * A class to represent paginated results from tag queries.
 * Includes both the list of tags and the pagination state for subsequent requests.
 */
class PaginatedTagResult {
    private final List<Tag> tags;
    private final String pagingState;
    
    public PaginatedTagResult(List<Tag> tags, String pagingState) {
        this.tags = tags;
        this.pagingState = pagingState;
    }
    
    public List<Tag> getTags() {
        return tags;
    }
    
    public String getPagingState() {
        return pagingState;
    }
    
    public boolean hasMorePages() {
        return pagingState != null && !pagingState.isEmpty();
    }
}