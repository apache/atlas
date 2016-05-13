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
package org.apache.atlas.hive.rewrite;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import java.util.HashMap;
import java.util.Map;

public class LiteralRewriter implements ASTRewriter {

    public static Map<Integer, String> LITERAL_TOKENS = new HashMap<Integer, String>() {{
        put(HiveParser.Number, "NUMBER_LITERAL");
        put(HiveParser.Digit, "DIGIT_LITERAL");
        put(HiveParser.HexDigit, "HEX_LITERAL");
        put(HiveParser.Exponent, "EXPONENT_LITERAL");
        put(HiveParser.StringLiteral, "'STRING_LITERAL'");
        put(HiveParser.BigintLiteral, "BIGINT_LITERAL");
        put(HiveParser.SmallintLiteral, "SMALLINT_LITERAL");
        put(HiveParser.TinyintLiteral, "TINYINT_LITERAL");
        put(HiveParser.DecimalLiteral, "DECIMAL_LITERAL");
        put(HiveParser.ByteLengthLiteral, "BYTE_LENGTH_LITERAL");
        put(HiveParser.TOK_STRINGLITERALSEQUENCE, "'STRING_LITERAL_SEQ'");
        put(HiveParser.TOK_CHARSETLITERAL, "'CHARSET_LITERAL'");
        put(HiveParser.KW_TRUE, "BOOLEAN_LITERAL");
        put(HiveParser.KW_FALSE, "BOOLEAN_LITERAL");
    }};


    @Override
    public void rewrite(RewriteContext ctx, final ASTNode node) throws RewriteException {
        try {
            processLiterals(ctx, node);
        } catch(Exception e) {
            throw new RewriteException("Could not normalize query", e);
        }
    }


    private void processLiterals(final RewriteContext ctx, final ASTNode node) {
        // Take child ident.totext
        if (isLiteral(node)) {
            replaceLiteral(ctx, node);
        }
    }

    private boolean isLiteral(ASTNode node) {
        if (LITERAL_TOKENS.containsKey(node.getType())) {
            return true;
        }
        return false;
    }

    void replaceLiteral(RewriteContext ctx, ASTNode valueNode) {
        //Reset the token stream
        String literalVal = LITERAL_TOKENS.get(valueNode.getType());
        ctx.getTokenRewriteStream().replace(valueNode.getTokenStartIndex(),
            valueNode.getTokenStopIndex(), literalVal);
    }
}
