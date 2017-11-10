/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.query;

import org.apache.atlas.query.Expressions.Expression;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


public class QueryParser {
    private static final Set<String> RESERVED_KEYWORDS =
            new HashSet<>(Arrays.asList("[", "]", "(", ")", "=", "<", ">", "!=", "<=", ">=", ",", "and", "or", "+", "-",
                                        "*", "/", ".", "select", "from", "where", "groupby", "loop", "isa", "is", "has",
                                        "as", "times", "withPath", "limit", "offset", "orderby", "count", "max", "min",
                                        "sum", "by", "order", "like"));

    public static boolean isKeyword(String word) {
        return RESERVED_KEYWORDS.contains(word);
    }

    public static Expression apply(String queryStr, QueryParams params) {
        Expression ret = null;

        return ret;
    }
}
