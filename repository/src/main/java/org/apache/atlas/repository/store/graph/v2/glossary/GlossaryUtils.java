package org.apache.atlas.repository.store.graph.v2.glossary;

import org.apache.atlas.util.NanoIdUtils;
import org.apache.commons.lang.StringUtils;

public class GlossaryUtils {

    public static final String ATLAS_GLOSSARY_TYPENAME          = "AtlasGlossary";
    public static final String ATLAS_GLOSSARY_TERM_TYPENAME     = "AtlasGlossaryTerm";
    public static final String ATLAS_GLOSSARY_CATEGORY_TYPENAME = "AtlasGlossaryCategory";

    public static final String NAME           = "name";
    public static final String QUALIFIED_NAME = "qualifiedName";

    public static final char[] invalidNameChars             = {'@'};

    // Relation name constants
    protected static final String ANCHOR                         = "anchor";
    protected static final String CATEGORY_PARENT                = "parentCategory";
    protected static final String CATEGORY_CHILDREN              = "childrenCategories";
    protected static final String ATLAS_GLOSSARY_PREFIX          = ATLAS_GLOSSARY_TYPENAME;
    protected static final String TERM_ANCHOR                    = ATLAS_GLOSSARY_PREFIX + "TermAnchor";
    protected static final String CATEGORY_ANCHOR                = ATLAS_GLOSSARY_PREFIX + "CategoryAnchor";
    protected static final String CATEGORY_HIERARCHY             = ATLAS_GLOSSARY_PREFIX + "CategoryHierarchyLink";
    protected static final String TERM_CATEGORIZATION            = ATLAS_GLOSSARY_PREFIX + "TermCategorization";
    protected static final String TERM_ASSIGNMENT                = ATLAS_GLOSSARY_PREFIX + "SemanticAssignment";


    protected static String getUUID(){
        return NanoIdUtils.randomNanoId();
    }

    protected static boolean isNameInvalid(String name) {
        return StringUtils.containsAny(name, invalidNameChars);
    }
}
