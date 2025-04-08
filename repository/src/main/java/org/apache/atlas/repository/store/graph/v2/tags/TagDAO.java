package org.apache.atlas.repository.store.graph.v2.tags;

import java.util.List;
import java.util.Map;

public interface TagDAO {
    List<AtlasTag> getTagsForVertex(String vertexId);
}