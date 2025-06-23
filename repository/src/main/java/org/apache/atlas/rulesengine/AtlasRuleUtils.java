
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.rulesengine;

import org.apache.atlas.model.instance.AtlasRule;
import org.apache.commons.collections.CollectionUtils;
import org.json.simple.JSONArray;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AtlasRuleUtils {
    public static final Comparator<AtlasRule>       ruleSorter                = (rule1, rule2) -> rule1.getCreatedTime().compareTo(rule2.getCreatedTime());
    static final        Function<Object[], Object>  isNullFunc                = (objarr) -> (objarr[0] == null);
    static final        Function<Object[], Object>  notNullFunc               = (objarr) -> (objarr[0] != null);
    static final        Function<Object[], Object>  startsWithFunc            = (objarr) -> ((String) objarr[0]).startsWith((String) objarr[1]);
    static final        Function<Object[], Object>  endsWithFunc              = (objarr) -> ((String) objarr[0]).endsWith((String) objarr[1]);
    static final        BiPredicate<String, String> containsBiPredicate       = AtlasRuleUtils::match;
    static final        BiPredicate<String, String> notContainsBiPredicate    = containsBiPredicate.negate();
    // Negating the containsBiPredicate to check if both strings do not match
    static final        Function<Object[], Object>  notContainsFunc           = (arr) -> (notContainsBiPredicate.test((String) arr[1], (String) arr[0]));
    static final        Function<Object[], Object>  notContainsIgnoreCaseFunc = (arr) -> (notContainsBiPredicate.test(((String) arr[1]).toLowerCase(), ((String) arr[0]).toLowerCase()));
    //checks if two given strings match. The chkStr(first parameter) may contain wildcard characters
    static final        Function<Object[], Object>  containsFunc              = (arr) -> (containsBiPredicate.test((String) arr[1], (String) arr[0]));
    static final        Function<Object[], Object>  containsIgnoreCaseFunc    = (arr) -> (containsBiPredicate.test(((String) arr[1]).toLowerCase(), ((String) arr[0]).toLowerCase()));
    static final        RuleAction                  ACCEPT                    = new RuleAction() {
        public RuleAction.Result performAction(Object dataObj) {
            return getResult();
        }

        public Result getResult() {
            return Result.ACCEPT;
        }
    };
    static final        RuleAction                  DISCARD                   = new RuleAction() {
        public RuleAction.Result performAction(Object dataObj) {
            return getResult();
        }

        public Result getResult() {
            return Result.DISCARD;
        }
    };

    private AtlasRuleUtils() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static String getRuleExprJsonString(AtlasRule.RuleExpr ruleExpr) {
        List<AtlasRule.RuleExprObject> exprList = ruleExpr.getRuleExprObjList();
        return JSONArray.toJSONString(exprList);
    }

    public static RuleAction getRuleActionFromString(String action) {
        boolean isAccept = RuleAction.Result.valueOf(action) == RuleAction.Result.ACCEPT;
        return (isAccept ? ACCEPT : DISCARD);
    }

    public static List<AtlasRule> getSortedRules(Collection<AtlasRule> ruleList) {
        List<AtlasRule> sortedRules = ruleList.stream()
                .sorted(AtlasRuleUtils.ruleSorter)
                .collect(Collectors.toList());

        return Collections.unmodifiableList(sortedRules);
    }

    public static boolean isDuplicateList(List<List<String>> allLists, List<String> listToCheck) {
        for (List<String> list : allLists) {
            if (CollectionUtils.isEqualCollection(list, listToCheck)) {
                return true;
            }
        }
        return false;
    }

    static boolean match(String strWithWildcard, String strTarget) {
        // If we reach at the end of both strings, we are done
        if (strWithWildcard.length() == 0 && strTarget.length() == 0) {
            return true;
        }

        //if no wildcard character, simply check contains substring
        if (!strWithWildcard.contains("*")) {
            return strTarget.contains(strWithWildcard);
        }

        // Make sure to eliminate consecutive '*'
        if (strWithWildcard.length() > 1 && strWithWildcard.charAt(0) == '*') {
            int i = 0;
            while (i + 1 < strWithWildcard.length() && strWithWildcard.charAt(i + 1) == '*') {
                i++;
            }
            strWithWildcard = strWithWildcard.substring(i);
        }

        // Make sure that the characters after '*' are present in second string. This function assumes that the strWithWildcard will not contain two consecutive '*'
        if (strWithWildcard.length() > 1 && strWithWildcard.charAt(0) == '*' && strTarget.length() == 0) {
            return false;
        }

        // If the first string contains '?', or current characters of both strings match
        if ((strWithWildcard.length() > 1 && strWithWildcard.charAt(0) == '?') || (strWithWildcard.length() != 0 && strTarget.length() != 0 && strWithWildcard.charAt(0) == strTarget.charAt(0))) {
            return match(strWithWildcard.substring(1), strTarget.substring(1));
        }
        // If there is *, then there are two possibilities
        // a) We consider current character of second string
        // b) We ignore current character of second string.
        if (strWithWildcard.length() > 0 && strWithWildcard.charAt(0) == '*') {
            return match(strWithWildcard.substring(1), strTarget) ||
                    match(strWithWildcard, strTarget.substring(1));
        }
        return false;
    }
}
