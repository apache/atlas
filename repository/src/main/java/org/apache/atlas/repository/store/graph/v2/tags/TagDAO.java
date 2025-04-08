package org.apache.atlas.repository.store.graph.v2.tags;

import org.apache.atlas.model.instance.AtlasClassification;

import java.util.List;

public interface TagDAO {
    List<AtlasClassification> getTagsForVertex(String vertexId);
}