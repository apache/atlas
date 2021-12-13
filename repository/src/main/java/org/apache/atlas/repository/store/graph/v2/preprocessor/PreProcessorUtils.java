package org.apache.atlas.repository.store.graph.v2.preprocessor;

import org.apache.atlas.util.NanoIdUtils;
import org.apache.commons.lang.StringUtils;

public class PreProcessorUtils {

    private static final char[] invalidNameChars = {'@'};

    //Glossary models constants
    public static final String ANCHOR            = "anchor";
    public static final String CATEGORY_PARENT   = "parentCategory";
    public static final String CATEGORY_CHILDREN = "childrenCategories";

    //Query models constants
    public static final String PREFIX_QUERY_QN   = "/default/collection/";
    public static final String COLLECTION_QUALIFIED_NAME = "collectionQualifiedName";


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
