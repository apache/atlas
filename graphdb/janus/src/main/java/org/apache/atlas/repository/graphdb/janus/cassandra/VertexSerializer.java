package org.apache.atlas.repository.graphdb.janus.cassandra;


import java.util.Map;

/**
 * Interface for serializing and deserializing vertex data.
 */
interface VertexSerializer {
    /**
     * Deserializes a JSON string into a DynamicVertex.
     *
     * @param jsonData The JSON data string
     * @return A DynamicVertex object
     */
    DynamicVertex deserialize(String jsonData);

    /**
     * Serializes a DynamicVertex into a JSON string.
     *
     * @param vertex The DynamicVertex to serialize
     * @return A JSON string representation
     */
    String serialize(DynamicVertex vertex);


    String serialize(Map<String, Object> object);


}