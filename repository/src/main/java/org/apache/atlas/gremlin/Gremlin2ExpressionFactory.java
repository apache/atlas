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

package org.apache.atlas.gremlin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.atlas.AtlasException;
import org.apache.atlas.groovy.AbstractFunctionExpression;
import org.apache.atlas.groovy.CastExpression;
import org.apache.atlas.groovy.ClosureExpression;
import org.apache.atlas.groovy.ComparisonExpression;
import org.apache.atlas.groovy.ComparisonExpression.ComparisonOperator;
import org.apache.atlas.groovy.ComparisonOperatorExpression;
import org.apache.atlas.groovy.FieldExpression;
import org.apache.atlas.groovy.FunctionCallExpression;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.IdentifierExpression;
import org.apache.atlas.groovy.ListExpression;
import org.apache.atlas.groovy.LiteralExpression;
import org.apache.atlas.groovy.LogicalExpression;
import org.apache.atlas.groovy.LogicalExpression.LogicalOperator;
import org.apache.atlas.groovy.RangeExpression;
import org.apache.atlas.groovy.TernaryOperatorExpression;
import org.apache.atlas.groovy.TraversalStepType;
import org.apache.atlas.query.Expressions;
import org.apache.atlas.query.GraphPersistenceStrategies;
import org.apache.atlas.query.TypeUtils.FieldInfo;
import org.apache.atlas.typesystem.types.IDataType;


/**
 * Generates gremlin query expressions using Gremlin 2 syntax.
 *
 */
public class Gremlin2ExpressionFactory extends GremlinExpressionFactory {

    private static final String LOOP_METHOD = "loop";
    private static final String CONTAINS = "contains";
    private static final String LOOP_COUNT_FIELD = "loops";
    private static final String PATH_FIELD = "path";
    private static final String ENABLE_PATH_METHOD = "enablePath";
    private static final String BACK_METHOD = "back";
    private static final String LAST_METHOD = "last";

    @Override
    public GroovyExpression generateLogicalExpression(GroovyExpression parent, String operator, List<GroovyExpression> operands) {
        return new FunctionCallExpression(TraversalStepType.FILTER, parent, operator, operands);
    }


    @Override
    public GroovyExpression generateBackReferenceExpression(GroovyExpression parent, boolean inSelect, String alias) {
        if (inSelect && parent == null) {
            return getFieldInSelect();
        }
        else if (inSelect && parent != null) {
            return parent;
        }
        else {
            return new FunctionCallExpression(TraversalStepType.MAP_TO_ELEMENT, parent, BACK_METHOD, new LiteralExpression(alias));
        }
    }

    @Override
    public GroovyExpression getLoopExpressionParent(GroovyExpression inputQry) {
        return inputQry;
    }

    @Override
    public GroovyExpression generateLoopExpression(GroovyExpression parent,GraphPersistenceStrategies s, IDataType dataType,  GroovyExpression loopExpr, String alias, Integer times) {

        GroovyExpression emitExpr = generateLoopEmitExpression(s, dataType);
        //note that in Gremlin 2 (unlike Gremlin 3), the parent is not explicitly used.  It is incorporated
        //in the loopExpr.
        GroovyExpression whileFunction = null;
        if(times != null) {
            GroovyExpression loopsExpr = new FieldExpression(getItVariable(), LOOP_COUNT_FIELD);
            GroovyExpression timesExpr = new LiteralExpression(times);
            whileFunction = new ClosureExpression(new ComparisonExpression(loopsExpr, ComparisonOperator.LESS_THAN, timesExpr));
        }
        else {
            GroovyExpression pathExpr = new FieldExpression(getItVariable(),PATH_FIELD);
            GroovyExpression itObjectExpr = getCurrentObjectExpression();
            GroovyExpression pathContainsExpr = new FunctionCallExpression(pathExpr, CONTAINS, itObjectExpr);
            whileFunction = new ClosureExpression(new TernaryOperatorExpression(pathContainsExpr, LiteralExpression.FALSE, LiteralExpression.TRUE));
        }
        GroovyExpression emitFunction = new ClosureExpression(emitExpr);
        GroovyExpression loopCall = new FunctionCallExpression(TraversalStepType.BRANCH, loopExpr, LOOP_METHOD, new LiteralExpression(alias), whileFunction, emitFunction);

        return new FunctionCallExpression(TraversalStepType.SIDE_EFFECT, loopCall, ENABLE_PATH_METHOD);
    }

    @Override
    public GroovyExpression typeTestExpression(GraphPersistenceStrategies s, String typeName, GroovyExpression itRef) {

        GroovyExpression superTypeAttrExpr = new FieldExpression(itRef, s.superTypeAttributeName());
        GroovyExpression typeNameExpr = new LiteralExpression(typeName);
        GroovyExpression isSuperTypeExpr = new FunctionCallExpression(superTypeAttrExpr, CONTAINS, typeNameExpr);
        GroovyExpression superTypeMatchesExpr = new TernaryOperatorExpression(superTypeAttrExpr, isSuperTypeExpr, LiteralExpression.FALSE);

        GroovyExpression typeAttrExpr = new FieldExpression(itRef, s.typeAttributeName());
        GroovyExpression typeMatchesExpr = new ComparisonExpression(typeAttrExpr, ComparisonOperator.EQUALS, typeNameExpr);
        return new LogicalExpression(typeMatchesExpr, LogicalOperator.OR, superTypeMatchesExpr);

    }

    @Override
    public GroovyExpression generateSelectExpression(GroovyExpression parent, List<LiteralExpression> sourceNames,
        List<GroovyExpression> srcExprs) {

        GroovyExpression srcNamesExpr = new ListExpression(sourceNames);
        List<GroovyExpression> selectArgs = new ArrayList<>();
        selectArgs.add(srcNamesExpr);
        for(GroovyExpression expr : srcExprs) {
            selectArgs.add(new ClosureExpression(expr));
        }
        return new FunctionCallExpression(TraversalStepType.MAP_TO_VALUE, parent, SELECT_METHOD, selectArgs);
    }

    @Override
    public GroovyExpression generateFieldExpression(GroovyExpression parent, FieldInfo fInfo, String propertyName, boolean inSelect) {
        return new FieldExpression(parent, propertyName);
    }

    @Override
    public GroovyExpression generateHasExpression(GraphPersistenceStrategies s, GroovyExpression parent, String propertyName, String symbol,
            GroovyExpression requiredValue, FieldInfo fInfo) throws AtlasException {
        GroovyExpression op = gremlin2CompOp(symbol);
        GroovyExpression propertyNameExpr = new LiteralExpression(propertyName);
        return new FunctionCallExpression(TraversalStepType.FILTER, parent, HAS_METHOD, propertyNameExpr, op, requiredValue);
    }

    @Override
    public GroovyExpression generateLikeExpressionUsingFilter(GroovyExpression parent, String propertyName, GroovyExpression propertyValue) throws AtlasException {
        GroovyExpression itExpr      = getItVariable();
        GroovyExpression nameExpr    = new FieldExpression(itExpr, propertyName);
        GroovyExpression matchesExpr = new FunctionCallExpression(nameExpr, MATCHES, escapePropertyValue(propertyValue));
        GroovyExpression closureExpr = new ClosureExpression(matchesExpr);
        GroovyExpression filterExpr  = new FunctionCallExpression(parent, FILTER_METHOD, closureExpr);

        return filterExpr;
    }

    private GroovyExpression escapePropertyValue(GroovyExpression propertyValue) {
        GroovyExpression ret = propertyValue;

        if (propertyValue instanceof LiteralExpression) {
            LiteralExpression exp = (LiteralExpression) propertyValue;

            if (exp != null && exp.getValue() instanceof String) {
                String stringValue = (String) exp.getValue();

                // replace '*' with ".*", replace '?' with '.'
                stringValue = stringValue.replaceAll("\\*", ".*")
                                         .replaceAll("\\?", ".");

                ret = new LiteralExpression(stringValue);
            }
        }

        return ret;
    }

    private GroovyExpression gremlin2CompOp(String op) throws AtlasException {

        GroovyExpression tExpr = new IdentifierExpression("T");
        if(op.equals("=")) {
            return new FieldExpression(tExpr, "eq");
        }
        if(op.equals("!=")) {
            return new FieldExpression(tExpr, "neq");
        }
        if(op.equals(">")) {
            return new FieldExpression(tExpr, "gt");
        }
        if(op.equals(">=")) {
            return new FieldExpression(tExpr, "gte");
        }
        if(op.equals("<")) {
            return new FieldExpression(tExpr, "lt");
        }
        if(op.equals("<=")) {
            return new FieldExpression(tExpr, "lte");
        }
        if(op.equals("in")) {
            return new FieldExpression(tExpr, "in");
        }
        throw new AtlasException("Comparison operator " + op + " not supported in Gremlin");
    }

    @Override
    protected GroovyExpression initialExpression(GroovyExpression varExpr, GraphPersistenceStrategies s) {
        return generateSeededTraversalExpresssion(false, varExpr);
    }

    @Override
    public GroovyExpression generateSeededTraversalExpresssion(boolean isMap, GroovyExpression varExpr) {
        return new FunctionCallExpression(TraversalStepType.START, varExpr, "_");
    }

    @Override
    public GroovyExpression generateRangeExpression(GroovyExpression parent, int startIndex, int endIndex) {
        //treat as barrier step, since limits need to be applied globally (even though it
        //is technically a filter step)
        return new RangeExpression(TraversalStepType.BARRIER, parent, startIndex, endIndex);
    }

    @Override
    public boolean isRangeExpression(GroovyExpression expr) {

        return (expr instanceof RangeExpression);
    }

    @Override
    public int[] getRangeParameters(AbstractFunctionExpression expr) {

        if (isRangeExpression(expr)) {
            RangeExpression rangeExpression = (RangeExpression) expr;
            return new int[] {rangeExpression.getStartIndex(), rangeExpression.getEndIndex()};
        }
        else {
            return null;
        }
    }

    @Override
    public void setRangeParameters(GroovyExpression expr, int startIndex, int endIndex) {

        if (isRangeExpression(expr)) {
            RangeExpression rangeExpression = (RangeExpression) expr;
            rangeExpression.setStartIndex(startIndex);
            rangeExpression.setEndIndex(endIndex);
        }
        else {
            throw new IllegalArgumentException(expr.getClass().getName() + " is not a valid range expression - must be an instance of " + RangeExpression.class.getName());
        }

    }

    @Override
    public List<GroovyExpression> getOrderFieldParents() {

        GroovyExpression itExpr = getItVariable();
        List<GroovyExpression> result = new ArrayList<>(2);
        result.add(new FieldExpression(itExpr, "a"));
        result.add(new FieldExpression(itExpr, "b"));
        return result;
    }


    @Override
    public GroovyExpression generateOrderByExpression(GroovyExpression parent, List<GroovyExpression> translatedOrderBy, boolean isAscending) {

        GroovyExpression aPropertyExpr = translatedOrderBy.get(0);
        GroovyExpression bPropertyExpr = translatedOrderBy.get(1);

        GroovyExpression aPropertyNotNull = new ComparisonExpression(aPropertyExpr, ComparisonOperator.NOT_EQUALS, LiteralExpression.NULL);
        GroovyExpression bPropertyNotNull = new ComparisonExpression(bPropertyExpr, ComparisonOperator.NOT_EQUALS, LiteralExpression.NULL);

        GroovyExpression aCondition = new TernaryOperatorExpression(aPropertyNotNull, new FunctionCallExpression(aPropertyExpr,TO_LOWER_CASE_METHOD), aPropertyExpr);
        GroovyExpression bCondition = new TernaryOperatorExpression(bPropertyNotNull, new FunctionCallExpression(bPropertyExpr,TO_LOWER_CASE_METHOD), bPropertyExpr);

        GroovyExpression comparisonFunction = null;
        if(isAscending) {
            comparisonFunction = new ComparisonOperatorExpression(aCondition, bCondition);
        }
        else {
            comparisonFunction = new ComparisonOperatorExpression(bCondition,  aCondition);
        }
        return new FunctionCallExpression(TraversalStepType.BARRIER, parent, ORDER_METHOD, new ClosureExpression(comparisonFunction));
    }


    @Override
    public GroovyExpression getAnonymousTraversalExpression() {
        return new FunctionCallExpression(TraversalStepType.START, "_");
    }



    @Override
    public GroovyExpression generateGroupByExpression(GroovyExpression parent, GroovyExpression groupByExpression,
                                                      GroovyExpression aggregationFunction) {
            GroovyExpression groupByClosureExpr = new ClosureExpression(groupByExpression);
            GroovyExpression itClosure = new ClosureExpression(getItVariable());
            GroovyExpression result = new FunctionCallExpression(TraversalStepType.BARRIER, parent, "groupBy", groupByClosureExpr, itClosure);
            result = new FunctionCallExpression(TraversalStepType.SIDE_EFFECT, result, "cap");
            result = new FunctionCallExpression(TraversalStepType.END, result, "next");
            result = new FunctionCallExpression(result, "values");
            result = new FunctionCallExpression(result, "toList");

            GroovyExpression aggregrationFunctionClosure = new ClosureExpression(aggregationFunction);
            result = new FunctionCallExpression(result, "collect", aggregrationFunctionClosure);
            return result;
    }

    @Override
    public GroovyExpression getFieldInSelect() {
        return getItVariable();
    }
    @Override
    public GroovyExpression getGroupBySelectFieldParent() {
        GroovyExpression itExpr = getItVariable();
        return new FunctionCallExpression(itExpr, LAST_METHOD);
    }

    //assumes cast already performed
    @Override
    public GroovyExpression generateCountExpression(GroovyExpression itExpr) {
        return new FunctionCallExpression(itExpr, "size");
    }

    @Override
    public String getTraversalExpressionClass() {
        return "GremlinPipeline";
    }


    @Override
    public boolean isSelectGeneratesMap(int aliasCount) {
        //in Gremlin 2 select always generates a map
        return true;
    }

    @Override
    public GroovyExpression generateMapExpression(GroovyExpression parent, ClosureExpression closureExpression) {
        return new FunctionCallExpression(TraversalStepType.MAP_TO_ELEMENT, parent, "transform", closureExpression);
    }

    @Override
    public GroovyExpression generateGetSelectedValueExpression(LiteralExpression key,
            GroovyExpression rowMap) {
        rowMap = new CastExpression(rowMap, "Row");
        GroovyExpression getExpr = new FunctionCallExpression(rowMap, "getColumn", key);
        return getExpr;
    }

    @Override
    public GroovyExpression getCurrentTraverserObject(GroovyExpression traverser) {
        return traverser;
    }

    public List<String> getAliasesRequiredByExpression(GroovyExpression expr) {
        if(!(expr instanceof FunctionCallExpression)) {
            return Collections.emptyList();
        }
        FunctionCallExpression fc = (FunctionCallExpression)expr;
        if(! fc.getFunctionName().equals(LOOP_METHOD)) {
            return Collections.emptyList();
        }
       LiteralExpression aliasName =  (LiteralExpression)fc.getArguments().get(0);
       return Collections.singletonList(aliasName.getValue().toString());
    }

    @Override
    public boolean isRepeatExpression(GroovyExpression expr) {
        if(!(expr instanceof FunctionCallExpression)) {
            return false;
        }
        return ((FunctionCallExpression)expr).getFunctionName().equals(LOOP_METHOD);
    }
}

