package org.apache.atlas.repository.graphdb.janus.cassandra;

import org.apache.atlas.exception.AtlasBaseException;

import java.util.List;
import java.util.Map;

/**
 * Repository interface for vertex data access.
 */
interface VertexDataRepository  {

    /**
     * Fetches vertex data as parsed JsonElements instead of raw strings.
     * This is more efficient when the caller needs to work with the JSON directly.
     *
     * @param vertexIds List of vertex IDs to fetch
     * @return Map of vertex ID to parsed JsonElement
     */

    Map<String, DynamicVertex> fetchVerticesDirectly(List<String> vertexIds) throws AtlasBaseException;

    void insertVertices(Map<String, String> serialisedVertices) throws AtlasBaseException;

    void dropVertices(List<String> vertexIds) throws AtlasBaseException;
}