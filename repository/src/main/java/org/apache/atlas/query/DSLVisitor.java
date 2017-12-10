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

public class DSLVisitor extends AtlasDSLParserBaseVisitor<String> {
    private final QueryProcessor queryProcessor;

    public DSLVisitor(QueryProcessor queryProcessor) {
        this.queryProcessor = queryProcessor;
    }

    @Override
    public String visitFromExpression(final FromExpressionContext ctx) {
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
    public String visitWhereClause(WhereClauseContext ctx) {
        ExprContext expr = ctx.expr();
        processExpr(expr.compE());

        if (CollectionUtils.isNotEmpty(expr.exprRight())) {
            for (ExprRightContext exprRight : expr.exprRight()) {
                if (exprRight.K_AND() != null) {
                    // AND expression
                    processExpr(exprRight.compE());
                }
                // OR is tricky
            }
        }
        return super.visitWhereClause(ctx);
    }

    private void processExpr(final CompEContext compE) {
        if (compE != null && compE.isClause() == null && compE.hasClause() == null && compE.isClause() == null) {
            ComparisonClauseContext comparisonClause = compE.comparisonClause();
            if(comparisonClause == null) {
                comparisonClause = compE.arithE().multiE().atomE().expr().compE().comparisonClause();
            }

            if (comparisonClause != null) {
                String lhs = comparisonClause.arithE(0).getText();
                String op = comparisonClause.operator().getText().toUpperCase();
                String rhs = comparisonClause.arithE(1).getText();

                queryProcessor.addWhere(lhs, op, rhs);
            }
        }
    }

    @Override
    public String visitSelectExpr(SelectExprContext ctx) {
        if (!(ctx.getParent() instanceof GroupByExpressionContext)) {
            String[] items = new String[ctx.selectExpression().size()];
            for (int i = 0; i < ctx.selectExpression().size(); i++) {
                items[i] = ctx.selectExpression(i).expr().getText();
            }

            queryProcessor.addSelect(items);
        }
        return super.visitSelectExpr(ctx);
    }

    @Override
    public String visitLimitOffset(LimitOffsetContext ctx) {
        queryProcessor.addLimit(ctx.limitClause().NUMBER().toString(),
                (ctx.offsetClause() == null ? "0" : ctx.offsetClause().NUMBER().getText()));
        return super.visitLimitOffset(ctx);
    }

    @Override
    public String visitOrderByExpr(OrderByExprContext ctx) {
        queryProcessor.addOrderBy(ctx.expr().getText(), (ctx.sortOrder() != null && ctx.sortOrder().getText().equalsIgnoreCase("desc")));
        return super.visitOrderByExpr(ctx);
    }

    @Override
    public String visitIsClause(IsClauseContext ctx) {
        queryProcessor.addFromIsA(ctx.arithE().getText(), ctx.identifier().getText());
        return super.visitIsClause(ctx);
    }

    @Override
    public String visitHasClause(HasClauseContext ctx) {
        queryProcessor.addFromProperty(ctx.arithE().getText(), ctx.identifier().getText());
        return super.visitHasClause(ctx);
    }

    @Override
    public String visitGroupByExpression(GroupByExpressionContext ctx) {
        String s = ctx.selectExpr().getText();
        queryProcessor.addGroupBy(s);
        return super.visitGroupByExpression(ctx);
    }
}
