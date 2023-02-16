package org.apache.atlas.repository.store.graph.v2.preprocessor;

import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.util.NanoIdUtils;
import org.apache.commons.lang.StringUtils;

import static org.apache.atlas.repository.Constants.ENTITY_TYPE_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;

public class PreProcessorUtils {

    private static final char[] invalidNameChars = {'@'};

    //Glossary models constants
    public static final String ANCHOR            = "anchor";
    public static final String CATEGORY_PARENT   = "parentCategory";
    public static final String CATEGORY_CHILDREN = "childrenCategories";

    //Query models constants
    public static final String PREFIX_QUERY_QN   = "/default/collection/";
    public static final String TYPE_COLLECTION = "Collection";
    public static final String COLLECTION_QUALIFIED_NAME = "collectionQualifiedName";
    public static final String PARENT_QUALIFIED_NAME = "parentQualifiedName";
    public static final String PARENT_ATTRIBUTE_NAME    = "parent";

    /**
     * Folder,Collection, Query relations
     */

    public static final String CHILDREN_QUERIES = "__Namespace.childrenQueries";
    public static final String CHILDREN_FOLDERS = "__Namespace.childrenFolders";

    public static String getUUID(){
        return NanoIdUtils.randomNanoId();
    }

    public static String getUserName(){
        return NanoIdUtils.randomNanoId();
    }

    public static boolean isNameInvalid(String name) {
        return StringUtils.containsAny(name, invalidNameChars);
    }

    public static String getCollectionPropertyName(AtlasVertex parentVertex) {
        return TYPE_COLLECTION.equals(parentVertex.getProperty(ENTITY_TYPE_PROPERTY_KEY, String.class)) ? QUALIFIED_NAME : COLLECTION_QUALIFIED_NAME;
    }
}
