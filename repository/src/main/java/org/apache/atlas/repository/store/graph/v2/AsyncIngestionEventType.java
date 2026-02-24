package org.apache.atlas.repository.store.graph.v2;

/**
 * Constants for async ingestion Kafka event types.
 * Centralises every event type string so consumers can switch on them reliably.
 */
public final class AsyncIngestionEventType {

    private AsyncIngestionEventType() {}

    // ---- Entity mutations (EntityMutationService) ----
    public static final String BULK_CREATE_OR_UPDATE          = "BULK_CREATE_OR_UPDATE";
    public static final String SET_CLASSIFICATIONS            = "SET_CLASSIFICATIONS";
    public static final String UPDATE_BY_UNIQUE_ATTRIBUTE     = "UPDATE_BY_UNIQUE_ATTRIBUTE";
    public static final String DELETE_BY_UNIQUE_ATTRIBUTE     = "DELETE_BY_UNIQUE_ATTRIBUTE";
    public static final String DELETE_BY_GUID                 = "DELETE_BY_GUID";
    public static final String DELETE_BY_GUIDS                = "DELETE_BY_GUIDS";
    public static final String RESTORE_BY_GUIDS               = "RESTORE_BY_GUIDS";
    public static final String BULK_DELETE_BY_UNIQUE_ATTRIBUTES = "BULK_DELETE_BY_UNIQUE_ATTRIBUTES";

    // ---- Classification mutations (EntityMutationService) ----
    public static final String ADD_CLASSIFICATIONS            = "ADD_CLASSIFICATIONS";
    public static final String UPDATE_CLASSIFICATIONS         = "UPDATE_CLASSIFICATIONS";
    public static final String DELETE_CLASSIFICATION          = "DELETE_CLASSIFICATION";
    public static final String ADD_CLASSIFICATION_BULK        = "ADD_CLASSIFICATION_BULK";

    // ---- Relationship mutations ----
    public static final String DELETE_RELATIONSHIP_BY_GUID    = "DELETE_RELATIONSHIP_BY_GUID";
    public static final String DELETE_RELATIONSHIPS_BY_GUIDS  = "DELETE_RELATIONSHIPS_BY_GUIDS";
    public static final String RELATIONSHIP_CREATE            = "RELATIONSHIP_CREATE";
    public static final String RELATIONSHIP_BULK_CREATE_OR_UPDATE = "RELATIONSHIP_BULK_CREATE_OR_UPDATE";
    public static final String RELATIONSHIP_UPDATE            = "RELATIONSHIP_UPDATE";

    // ---- Business metadata mutations (EntityREST) ----
    public static final String ADD_OR_UPDATE_BUSINESS_ATTRIBUTES              = "ADD_OR_UPDATE_BUSINESS_ATTRIBUTES";
    public static final String ADD_OR_UPDATE_BUSINESS_ATTRIBUTES_BY_DISPLAY_NAME = "ADD_OR_UPDATE_BUSINESS_ATTRIBUTES_BY_DISPLAY_NAME";
    public static final String REMOVE_BUSINESS_ATTRIBUTES     = "REMOVE_BUSINESS_ATTRIBUTES";

    // ---- TypeDef mutations (TypesREST) ----
    public static final String TYPEDEF_CREATE                 = "TYPEDEF_CREATE";
    public static final String TYPEDEF_UPDATE                 = "TYPEDEF_UPDATE";
    public static final String TYPEDEF_DELETE                 = "TYPEDEF_DELETE";
    public static final String TYPEDEF_DELETE_BY_NAME         = "TYPEDEF_DELETE_BY_NAME";
}
