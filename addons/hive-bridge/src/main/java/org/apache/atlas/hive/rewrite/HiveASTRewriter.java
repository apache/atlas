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


import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HiveASTRewriter {

    private Context queryContext;
    private RewriteContext rwCtx;
    private List<ASTRewriter> rewriters = new ArrayList<>();

    private static final Logger LOG = LoggerFactory.getLogger(HiveASTRewriter.class);

    public HiveASTRewriter(HiveConf conf) throws RewriteException {
        try {
            queryContext = new Context(conf);
            setUpRewriters();
        } catch (IOException e) {
            throw new RewriteException("Exception while rewriting query : " , e);
        }
    }

    private void setUpRewriters() throws RewriteException {
        ASTRewriter rewriter = new LiteralRewriter();
        rewriters.add(rewriter);
    }

    public String rewrite(String sourceQry) throws RewriteException {
        String result = sourceQry;
        ASTNode tree = null;
        try {
            ParseDriver pd = new ParseDriver();
            tree = pd.parse(sourceQry, queryContext, true);
            tree = ParseUtils.findRootNonNullToken(tree);
            this.rwCtx = new RewriteContext(sourceQry, tree, queryContext.getTokenRewriteStream());
            rewrite(tree);
            result = toSQL();
        } catch (ParseException e) {
           LOG.error("Could not parse the query {} ", sourceQry, e);
            throw new RewriteException("Could not parse query : " , e);
        }
        return result;
    }

    private void rewrite(ASTNode origin) throws RewriteException {
        ASTNode node = origin;
        if (node != null) {
            for(ASTRewriter rewriter : rewriters) {
                rewriter.rewrite(rwCtx, node);
            }
            if (node.getChildren() != null) {
                for (int i = 0; i < node.getChildren().size(); i++) {
                    rewrite((ASTNode) node.getChild(i));
                }
            }
        }
    }

    public String toSQL() {
        return rwCtx.getTokenRewriteStream().toString();
    }

    public String printAST() {
        return rwCtx.getOriginNode().dump();
    }

}
