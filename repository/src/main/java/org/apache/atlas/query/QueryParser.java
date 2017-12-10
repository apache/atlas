/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.query;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenStream;
import org.apache.atlas.query.Expressions.Expression;
import org.apache.atlas.query.antlr4.AtlasDSLLexer;
import org.apache.atlas.query.antlr4.AtlasDSLParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class QueryParser {
    private static final Logger LOG = LoggerFactory.getLogger(QueryParser.class);

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

        try {
            InputStream    stream           = new ByteArrayInputStream(queryStr.getBytes());
            AtlasDSLLexer lexer            = new AtlasDSLLexer(CharStreams.fromStream(stream));
            TokenStream    inputTokenStream = new CommonTokenStream(lexer);
            AtlasDSLParser parser           = new AtlasDSLParser(inputTokenStream);

            ret = new Expression(parser.query());
        } catch (IOException e) {
            ret = null;
            LOG.error(e.getMessage(), e);
        }

        return ret;
    }
}
