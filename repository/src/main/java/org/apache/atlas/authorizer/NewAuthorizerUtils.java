package org.apache.atlas.authorizer;

import org.apache.atlas.authorizer.authorizers.ListAuthorizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class NewAuthorizerUtils {
    private static final Logger LOG = LoggerFactory.getLogger(NewAuthorizerUtils.class);

    public static final String POLICY_TYPE_ALLOW = "allow";
    public static final String POLICY_TYPE_DENY = "deny";
    public static final int MAX_CLAUSE_LIMIT = 1024;

    public static final String DENY_POLICY_NAME_SUFFIX = "_deny";

    public static Map<String, Object> getPreFilterDsl(String persona, String purpose, List<String> actions) {
        return ListAuthorizer.getElasticsearchDSL(persona, purpose, actions);
    }
}
