package org.apache.atlas.repository.graphdb.janus;

import com.google.common.collect.ImmutableSet;
import java.util.Set;

public class AtlasRelationshipConstants {
    public static final String GUID_KEY = "__guid";
    public static final String OPPOSITE_END_TYPENAME = "__typeName";
    public  static final String END_DEF_NAME = "endDefName";
    public  static final String RELATIONSHIPS_PARAMS_KEY = "relationships";
    public  static final String RELATIONSHIP_GUID_KEY = "relationshipGuid";
    public  static final String RELATIONSHIPS_TYPENAME_KEY = "relationshipTypeName";
    public static final String IS_BI_DIRECTIONAL_RELATIONSHIP_MAPPING = "atlas.index.relationships.bidirectional";
    public static final Set<String> RELATIONSHIPS_TYPES_SUPPORTED = ImmutableSet.of("asset_readme", "RelatedReadme", "asset_links", "AtlasGlossarySemanticAssignment", "AtlasGlossarySynonym", "AtlasGlossaryAntonym", "table_columns");

}