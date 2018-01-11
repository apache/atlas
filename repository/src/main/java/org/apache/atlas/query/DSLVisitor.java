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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.query.antlr4.AtlasDSLParser.RULE_whereClause;

public class DSLVisitor extends AtlasDSLParserBaseVisitor<Void> {
    private static final Logger LOG = LoggerFactory.getLogger(DSLVisitor.class);

    private static final String AND = "AND";
    private static final String OR  = "OR";

    private Set<Integer> visitedRuleIndexes = new HashSet<>();
    private final GremlinQueryComposer gremlinQueryComposer;

    public DSLVisitor(GremlinQueryComposer gremlinQueryComposer) {
        this.gremlinQueryComposer = gremlinQueryComposer;
    }

    @Override
    public Void visitSpaceDelimitedQueries(SpaceDelimitedQueriesContext ctx) {
        addVisitedRule(ctx.getRuleIndex());
        return super.visitSpaceDelimitedQueries(ctx);
    }

    @Override
    public Void visitCommaDelimitedQueries(CommaDelimitedQueriesContext ctx) {
        addVisitedRule(ctx.getRuleIndex());
        return super.visitCommaDelimitedQueries(ctx);
    }

    @Override
    public Void visitIsClause(IsClauseContext ctx) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("=> DSLVisitor.visitIsClause({})", ctx);
        }

        if(!hasVisitedRule(RULE_whereClause)) {
            gremlinQueryComposer.addFromIsA(ctx.arithE().getText(), ctx.identifier().getText());
        }

        return super.visitIsClause(ctx);
    }

    @Override
    public Void visitHasClause(HasClauseContext ctx) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("=> DSLVisitor.visitHasClause({})", ctx);
        }

        if(!hasVisitedRule(RULE_whereClause)) {
            gremlinQueryComposer.addFromProperty(ctx.arithE().getText(), ctx.identifier().getText());
        }

        return super.visitHasClause(ctx);
    }

    @Override
    public Void visitLimitOffset(LimitOffsetContext ctx) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("=> DSLVisitor.visitLimitOffset({})", ctx);
        }

        gremlinQueryComposer.addLimit(ctx.limitClause().NUMBER().getText(),
                                      (ctx.offsetClause() == null ? "0" : ctx.offsetClause().NUMBER().getText()));
        return super.visitLimitOffset(ctx);
    }

    @Override
    public Void visitSelectExpr(SelectExprContext ctx) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("=> DSLVisitor.visitSelectExpr({})", ctx);
        }

        // Select can have only attributes, aliased attributes or aggregate functions

        // Groupby attr also represent select expr, no processing is needed in that case
        // visit groupBy would handle the select expr appropriately
        if (!(ctx.getParent() instanceof GroupByExpressionContext)) {
            String[] items  = new String[ctx.selectExpression().size()];
            String[] labels = new String[ctx.selectExpression().size()];

            SelectClauseComposer selectClauseComposer = new SelectClauseComposer();

            for (int i = 0; i < ctx.selectExpression().size(); i++) {
                SelectExpressionContext selectExpression = ctx.selectExpression(i);
                CountClauseContext      countClause      = selectExpression.expr().compE().countClause();
                SumClauseContext        sumClause        = selectExpression.expr().compE().sumClause();
                MinClauseContext        minClause        = selectExpression.expr().compE().minClause();
                MaxClauseContext        maxClause        = selectExpression.expr().compE().maxClause();
                IdentifierContext       identifier       = selectExpression.identifier();

                labels[i] = identifier != null ? identifier.getText() : selectExpression.getText();

                if (Objects.nonNull(countClause)) {
                    items[i] = "count";
                    selectClauseComposer.setCountIdx(i);
                } else if (Objects.nonNull(sumClause)) {
                    items[i] = sumClause.expr().getText();
                    selectClauseComposer.setSumIdx(i);
                } else if (Objects.nonNull(minClause)) {
                    items[i] = minClause.expr().getText();
                    selectClauseComposer.setMinIdx(i);
                } else if (Objects.nonNull(maxClause)) {
                    items[i] = maxClause.expr().getText();
                    selectClauseComposer.setMaxIdx(i);
                } else {
                    items[i] = selectExpression.expr().getText();
                }
            }

            selectClauseComposer.setItems(items);
            selectClauseComposer.setAttributes(items);
            selectClauseComposer.setLabels(labels);
            gremlinQueryComposer.addSelect(selectClauseComposer);
        }
        return super.visitSelectExpr(ctx);
    }

    @Override
    public Void visitOrderByExpr(OrderByExprContext ctx) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("=> DSLVisitor.visitOrderByExpr({})", ctx);
        }

        // Extract the attribute from parentheses
        String text = ctx.expr().getText().replace("(", "").replace(")", "");
        gremlinQueryComposer.addOrderBy(text, (ctx.sortOrder() != null && ctx.sortOrder().getText().equalsIgnoreCase("desc")));
        return super.visitOrderByExpr(ctx);
    }

    @Override
    public Void visitWhereClause(WhereClauseContext ctx) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("=> DSLVisitor.visitWhereClause({})", ctx);
        }

        addVisitedRule(ctx.getRuleIndex());
        ExprContext expr = ctx.expr();
        processExpr(expr, gremlinQueryComposer);
        return super.visitWhereClause(ctx);
    }

    @Override
    public Void visitFromExpression(final FromExpressionContext ctx) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("=> DSLVisitor.visitFromExpression({})", ctx);
        }

        FromSrcContext fromSrc = ctx.fromSrc();
        AliasExprContext aliasExpr = fromSrc.aliasExpr();

        if (aliasExpr != null) {
            gremlinQueryComposer.addFromAlias(aliasExpr.identifier(0).getText(), aliasExpr.identifier(1).getText());
        } else {
            if (fromSrc.identifier() != null) {
                gremlinQueryComposer.addFrom(fromSrc.identifier().getText());
            } else {
                gremlinQueryComposer.addFrom(fromSrc.literal().getText());
            }
        }
        return super.visitFromExpression(ctx);
    }

    @Override
    public Void visitSingleQrySrc(SingleQrySrcContext ctx) {
        if (!hasVisitedRule(RULE_whereClause)) {
            if (ctx.fromExpression() == null) {
                if (ctx.expr() != null && gremlinQueryComposer.hasFromClause()) {
                    processExpr(ctx.expr(), gremlinQueryComposer);
                }
            }
        }

        return super.visitSingleQrySrc(ctx);
    }

    @Override
    public Void visitGroupByExpression(GroupByExpressionContext ctx) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("=> DSLVisitor.visitGroupByExpression({})", ctx);
        }

        String s = ctx.selectExpr().getText();
        gremlinQueryComposer.addGroupBy(s);
        return super.visitGroupByExpression(ctx);
    }

    private void processExpr(final ExprContext expr, GremlinQueryComposer gremlinQueryComposer) {
        if (CollectionUtils.isNotEmpty(expr.exprRight())) {
            processExprRight(expr, gremlinQueryComposer);
        } else {
            processExpr(expr.compE(), gremlinQueryComposer);
        }
    }

    private void processExprRight(final ExprContext expr, GremlinQueryComposer gremlinQueryComposer) {
        GremlinQueryComposer nestedProcessor = gremlinQueryComposer.createNestedProcessor();

        List<String> nestedQueries = new ArrayList<>();
        String       prev          = null;

        // Process first expression then proceed with the others
        // expr -> compE exprRight*
        processExpr(expr.compE(), nestedProcessor);
        nestedQueries.add(nestedProcessor.get());

        for (ExprRightContext exprRight : expr.exprRight()) {
            nestedProcessor = gremlinQueryComposer.createNestedProcessor();

            // AND expression
            if (exprRight.K_AND() != null) {
                if (prev == null) prev = AND;
                if (OR.equalsIgnoreCase(prev)) {
                    // Change of context
                    GremlinQueryComposer orClause = nestedProcessor.createNestedProcessor();
                    orClause.addOrClauses(nestedQueries);
                    nestedQueries.clear();
                    nestedQueries.add(orClause.get());
                }
                prev = AND;
            }
            // OR expression
            if (exprRight.K_OR() != null) {
                if (prev == null) prev = OR;
                if (AND.equalsIgnoreCase(prev)) {
                    // Change of context
                    GremlinQueryComposer andClause = nestedProcessor.createNestedProcessor();
                    andClause.addAndClauses(nestedQueries);
                    nestedQueries.clear();
                    nestedQueries.add(andClause.get());
                }
                prev = OR;
            }
            processExpr(exprRight.compE(), nestedProcessor);
            nestedQueries.add(nestedProcessor.get());
        }
        if (AND.equalsIgnoreCase(prev)) {
            gremlinQueryComposer.addAndClauses(nestedQueries);
        }
        if (OR.equalsIgnoreCase(prev)) {
            gremlinQueryComposer.addOrClauses(nestedQueries);
        }
    }

    private void processExpr(final CompEContext compE, final GremlinQueryComposer gremlinQueryComposer) {
        if (compE != null && compE.isClause() == null && compE.hasClause() == null) {
            ComparisonClauseContext comparisonClause = compE.comparisonClause();

            // The nested expression might have ANDs/ORs
            if (comparisonClause == null) {
                ExprContext exprContext = compE.arithE().multiE().atomE().expr();
                // Only extract comparison clause if there are no nested exprRight clauses
                if (CollectionUtils.isEmpty(exprContext.exprRight())) {
                    comparisonClause = exprContext.compE().comparisonClause();
                }
            }

            if (comparisonClause != null) {
                String lhs = comparisonClause.arithE(0).getText();
                String op, rhs;
                AtomEContext atomECtx = comparisonClause.arithE(1).multiE().atomE();
                if (atomECtx.literal() == null ||
                        (atomECtx.literal() != null && atomECtx.literal().valueArray() == null)) {
                    op = comparisonClause.operator().getText().toUpperCase();
                    rhs = comparisonClause.arithE(1).getText();
                } else {
                    op = "in";
                    rhs = getInClause(atomECtx);
                }

                gremlinQueryComposer.addWhere(lhs, op, rhs);
            } else {
                processExpr(compE.arithE().multiE().atomE().expr(), gremlinQueryComposer);
            }
        }

        if (compE != null && compE.isClause() != null) {
            gremlinQueryComposer.addFromIsA(compE.isClause().arithE().getText(), compE.isClause().identifier().getText());
        }

        if (compE != null && compE.hasClause() != null) {
            gremlinQueryComposer.addFromProperty(compE.hasClause().arithE().getText(), compE.hasClause().identifier().getText());
        }
    }

    private String getInClause(AtomEContext atomEContext) {
        StringBuilder sb = new StringBuilder();
        ValueArrayContext valueArrayContext = atomEContext.literal().valueArray();
        int startIdx = 1;
        int endIdx = valueArrayContext.children.size() - 1;
        for (int i = startIdx; i < endIdx; i++) {
            sb.append(valueArrayContext.getChild(i));
        }

        return sb.toString();
    }

    private void addVisitedRule(int ruleIndex) {
        visitedRuleIndexes.add(ruleIndex);
    }

    private boolean hasVisitedRule(int ruleIndex) {
        return visitedRuleIndexes.contains(ruleIndex);
    }
}
