package org.apache.atlas.model.instance;

import org.apache.commons.collections.CollectionUtils;
import org.apache.curator.shaded.com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

public final class RelationshipMutationResponse {

    private final List<AtlasRelationship> createdRelationships;
    private final List<AtlasRelationship> updatedRelationships;
    private final List<AtlasRelationship> deletedRelationships;

    private RelationshipMutationResponse(List<AtlasRelationship> createdRelationships, List<AtlasRelationship> updatedRelationships, List<AtlasRelationship> deletedRelationships) {
        this.createdRelationships = CollectionUtils.isNotEmpty(createdRelationships) ? ImmutableList.copyOf(createdRelationships) : new ArrayList<>();
        this.updatedRelationships = CollectionUtils.isNotEmpty(updatedRelationships) ? ImmutableList.copyOf(updatedRelationships) : new ArrayList<>();
        this.deletedRelationships = CollectionUtils.isNotEmpty(deletedRelationships) ? ImmutableList.copyOf(deletedRelationships) : new ArrayList<>();
    }

    public static RelationshipMutationResponse getInstance(List<AtlasRelationship> createdRelationships, List<AtlasRelationship> updatedRelationships, List<AtlasRelationship> deletedRelationships) {
        return new RelationshipMutationResponse(createdRelationships, updatedRelationships, deletedRelationships);
    }

    public List<AtlasRelationship> getCreatedRelationships() {
        return createdRelationships;
    }

    public List<AtlasRelationship> getUpdatedRelationships() {
        return updatedRelationships;
    }

    public List<AtlasRelationship> getDeletedRelationships() {
        return deletedRelationships;
    }
}
