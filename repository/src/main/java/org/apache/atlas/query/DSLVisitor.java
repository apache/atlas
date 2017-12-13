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

import org.apache.atlas.query.antlr4.AtlasDSLParser.*;
import org.apache.atlas.query.antlr4.AtlasDSLParserBaseVisitor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DSLVisitor extends AtlasDSLParserBaseVisitor<String> {
    private static final Logger LOG = LoggerFactory.getLogger(DSLVisitor.class);

    private static final String AND = "AND";
    private static final String OR  = "OR";

    private final QueryProcessor queryProcessor;

    public DSLVisitor(QueryProcessor queryProcessor) {
        this.queryProcessor = queryProcessor;
    }

    @Override
    public String visitIsClause(IsClauseContext ctx) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("=> DSLVisitor.visitIsClause({})", ctx);
        }

        queryProcessor.addFromIsA(ctx.arithE().getText(), ctx.identifier().getText());
        return super.visitIsClause(ctx);
    }

    @Override
    public String visitHasClause(HasClauseContext ctx) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("=> DSLVisitor.visitHasClause({})", ctx);
        }

        queryProcessor.addFromProperty(ctx.arithE().getText(), ctx.identifier().getText());
        return super.visitHasClause(ctx);
    }

    @Override
    public String visitLimitOffset(LimitOffsetContext ctx) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("=> DSLVisitor.visitLimitOffset({})", ctx);
        }

        queryProcessor.addLimit(ctx.limitClause().NUMBER().toString(),
                (ctx.offsetClause() == null ? "0" : ctx.offsetClause().NUMBER().getText()));
        return super.visitLimitOffset(ctx);
    }

    @Override
    public String visitSelectExpr(SelectExprContext ctx) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("=> DSLVisitor.visitSelectExpr({})", ctx);
        }

        if (!(ctx.getParent() instanceof GroupByExpressionContext)) {
            List<Pair<String, String>> items = new ArrayList<>();
            for (int i = 0; i < ctx.selectExpression().size(); i++) {
                String idf = ctx.selectExpression(i).expr().getText();
                String alias = (ctx.selectExpression(i).K_AS() != null) ?
                        ctx.selectExpression(i).identifier().getText() : "";

                items.add(new MutablePair<String, String>(idf, alias));
            }

            queryProcessor.addSelect(items);
        }
        return super.visitSelectExpr(ctx);
    }

    @Override
    public String visitOrderByExpr(OrderByExprContext ctx) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("=> DSLVisitor.visitOrderByExpr({})", ctx);
        }

        queryProcessor.addOrderBy(ctx.expr().getText(), (ctx.sortOrder() != null && ctx.sortOrder().getText().equalsIgnoreCase("desc")));
        return super.visitOrderByExpr(ctx);
    }

    @Override
    public String visitWhereClause(WhereClauseContext ctx) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("=> DSLVisitor.visitWhereClause({})", ctx);
        }


        // The first expr shouldn't be processed if there are following exprs
        ExprContext expr = ctx.expr();

        processExpr(expr, queryProcessor);
        return super.visitWhereClause(ctx);
    }

    @Override
    public String visitFromExpression(final FromExpressionContext ctx) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("=> DSLVisitor.visitFromExpression({})", ctx);
        }

        FromSrcContext fromSrc = ctx.fromSrc();
        AliasExprContext aliasExpr = fromSrc.aliasExpr();

        if (aliasExpr != null) {
            queryProcessor.addFromAlias(aliasExpr.identifier(0).getText(), aliasExpr.identifier(1).getText());
        } else {
            if (fromSrc.identifier() != null) {
                queryProcessor.addFrom(fromSrc.identifier().getText());
            } else {
                queryProcessor.addFrom(fromSrc.literal().getText());
            }
        }
        return super.visitFromExpression(ctx);
    }

    @Override
    public String visitGroupByExpression(GroupByExpressionContext ctx) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("=> DSLVisitor.visitGroupByExpression({})", ctx);
        }

        String s = ctx.selectExpr().getText();
        queryProcessor.addGroupBy(s);
        return super.visitGroupByExpression(ctx);
    }

    private void processExpr(final ExprContext expr, QueryProcessor queryProcessor) {
        if (CollectionUtils.isNotEmpty(expr.exprRight())) {
            processExprRight(expr, queryProcessor);
        } else {
            processExpr(expr.compE(), queryProcessor);
        }
    }

    private void processExprRight(final ExprContext expr, QueryProcessor queryProcessor) {
        QueryProcessor nestedProcessor = queryProcessor.createNestedProcessor();

        List<String> nestedQueries = new ArrayList<>();
        String       prev          = null;

        // Process first expression then proceed with the others
        // expr -> compE exprRight*
        processExpr(expr.compE(), nestedProcessor);
        nestedQueries.add(nestedProcessor.getText());

        for (ExprRightContext exprRight : expr.exprRight()) {
            nestedProcessor = queryProcessor.createNestedProcessor();

            // AND expression
            if (exprRight.K_AND() != null) {
                if (prev == null) prev = AND;
                if (OR.equalsIgnoreCase(prev)) {
                    // Change of context
                    QueryProcessor orClause = nestedProcessor.createNestedProcessor();
                    orClause.addOrClauses(nestedQueries);
                    nestedQueries.clear();
                    nestedQueries.add(orClause.getText());
                }
                prev = AND;
            }
            // OR expression
            if (exprRight.K_OR() != null) {
                if (prev == null) prev = OR;
                if (AND.equalsIgnoreCase(prev)) {
                    // Change of context
                    QueryProcessor andClause = nestedProcessor.createNestedProcessor();
                    andClause.addAndClauses(nestedQueries);
                    nestedQueries.clear();
                    nestedQueries.add(andClause.getText());
                }
                prev = OR;
            }
            processExpr(exprRight.compE(), nestedProcessor);
            nestedQueries.add(nestedProcessor.getText());
        }
        if (AND.equalsIgnoreCase(prev)) {
            queryProcessor.addAndClauses(nestedQueries);
        }
        if (OR.equalsIgnoreCase(prev)) {
            queryProcessor.addOrClauses(nestedQueries);
        }
    }

    private void processExpr(final CompEContext compE, final QueryProcessor queryProcessor) {
        if (compE != null && compE.isClause() == null && compE.hasClause() == null && compE.isClause() == null) {
            ComparisonClauseContext comparisonClause = compE.comparisonClause();

            // The nested expression might have ANDs/ORs
            if(comparisonClause == null) {
                ExprContext exprContext = compE.arithE().multiE().atomE().expr();
                // Only extract comparison clause if there are no nested exprRight clauses
                if (CollectionUtils.isEmpty(exprContext.exprRight())) {
                    comparisonClause = exprContext.compE().comparisonClause();
                }
            }

            if (comparisonClause != null) {
                String lhs = comparisonClause.arithE(0).getText();
                String op  = comparisonClause.operator().getText().toUpperCase();
                String rhs = comparisonClause.arithE(1).getText();

                queryProcessor.addWhere(lhs, op, rhs);
            } else {
                processExpr(compE.arithE().multiE().atomE().expr(), queryProcessor);
            }
        }
    }
}
