package org.apache.atlas.repository.store.graph.v2.preprocessor;

import org.apache.atlas.util.NanoIdUtils;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;

public class PreProcessorUtils {

    private static final char[] invalidNameChars = {'@'};

    //Glossary models constants
    public static final String ANCHOR            = "anchor";
    public static final String CATEGORY_PARENT   = "parentCategory";
    public static final String CATEGORY_CHILDREN = "childrenCategories";

    //Query models constants
    public static final String PREFIX_QUERY_QN   = "/default/collection/";
    public static final String COLLECTION_QUALIFIED_NAME = "collectionQualifiedName";
    public static final String PARENT_QUALIFIED_NAME = "parentQualifiedName";
    public static final String PARENT_ATTRIBUTE_NAME    = "parent";

    /**
     * Folder,Collection, Query relations
     */

    public static final String CHILDREN_QUERIES = "__Namespace.childrenQueries";

    public static final List<String> QUERY_COLLECTION_RELATED_STRING_ATTRIBUTES = new ArrayList<String>(){{
        add(COLLECTION_QUALIFIED_NAME);
        add(PARENT_QUALIFIED_NAME);
    }};


    public static String getUUID(){
        return NanoIdUtils.randomNanoId();
    }

    public static String getUserName(){
        return NanoIdUtils.randomNanoId();
    }

    public static boolean isNameInvalid(String name) {
        return StringUtils.containsAny(name, invalidNameChars);
    }
}
