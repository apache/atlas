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
import org.apache.atlas.groovy.LiteralExpression;
import org.apache.atlas.groovy.LogicalExpression;
import org.apache.atlas.groovy.LogicalExpression.LogicalOperator;
import org.apache.atlas.groovy.TernaryOperatorExpression;
import org.apache.atlas.groovy.TraversalStepType;
import org.apache.atlas.groovy.TypeCoersionExpression;
import org.apache.atlas.query.GraphPersistenceStrategies;
import org.apache.atlas.query.TypeUtils.FieldInfo;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.IDataType;

/**
 * Generates gremlin query expressions using Gremlin 3 syntax.
 *
 */
public class Gremlin3ExpressionFactory extends GremlinExpressionFactory {



    private static final String VERTEX_LIST_CLASS = "List<Vertex>";
    private static final String VERTEX_ARRAY_CLASS = "Vertex[]";
    private static final String OBJECT_ARRAY_CLASS = "Object[]";
    private static final String VERTEX_CLASS = "Vertex";
    private static final String FUNCTION_CLASS = "Function";

    private static final String VALUE_METHOD = "value";
    private static final String IS_PRESENT_METHOD = "isPresent";
    private static final String MAP_METHOD = "map";
    private static final String VALUES_METHOD = "values";
    private static final String GET_METHOD = "get";
    private static final String OR_ELSE_METHOD = "orElse";
    private static final String PROPERTY_METHOD = "property";
    private static final String BY_METHOD = "by";
    private static final String EQ_METHOD = "eq";
    private static final String EMIT_METHOD = "emit";
    private static final String TIMES_METHOD = "times";
    private static final String REPEAT_METHOD = "repeat";
    private static final String RANGE_METHOD = "range";
    private static final String LAST_METHOD = "last";
    private static final String TO_STRING_METHOD = "toString";

    private static final GroovyExpression EMPTY_STRING_EXPRESSION = new LiteralExpression("");

    @Override
    public GroovyExpression generateLogicalExpression(GroovyExpression parent, String operator,
                                                      List<GroovyExpression> operands) {
        return new FunctionCallExpression(TraversalStepType.FILTER, parent, operator, operands);
    }

    @Override
    public GroovyExpression generateBackReferenceExpression(GroovyExpression parent, boolean inSelect, String alias) {
        if (inSelect) {
            return getFieldInSelect();
        } else {
            return new FunctionCallExpression(TraversalStepType.MAP_TO_ELEMENT, parent, SELECT_METHOD, new LiteralExpression(alias));
        }
    }

    @Override
    public GroovyExpression typeTestExpression(GraphPersistenceStrategies s, String typeName, GroovyExpression itRef) {
        LiteralExpression superTypeAttrExpr = new LiteralExpression(s.superTypeAttributeName());
        LiteralExpression typeNameExpr = new LiteralExpression(typeName);
        LiteralExpression typeAttrExpr = new LiteralExpression(s.typeAttributeName());
        FunctionCallExpression result = new FunctionCallExpression(TraversalStepType.FILTER, HAS_METHOD, typeAttrExpr, new FunctionCallExpression(EQ_METHOD, typeNameExpr));
        result = new FunctionCallExpression(TraversalStepType.FILTER, result, "or");
        result = new FunctionCallExpression(TraversalStepType.FILTER, result, HAS_METHOD, superTypeAttrExpr, new FunctionCallExpression(EQ_METHOD, typeNameExpr));
        return result;

    }

    @Override
    public GroovyExpression generateLoopExpression(GroovyExpression parent,GraphPersistenceStrategies s, IDataType dataType,  GroovyExpression loopExpr, String alias, Integer times) {

        GroovyExpression emitExpr = generateLoopEmitExpression(s, dataType);

        GroovyExpression result = new FunctionCallExpression(TraversalStepType.BRANCH, parent, REPEAT_METHOD, loopExpr);
        if (times != null) {
            GroovyExpression timesExpr = new LiteralExpression(times);
            result = new FunctionCallExpression(TraversalStepType.SIDE_EFFECT, result, TIMES_METHOD, timesExpr);
        }
        result = new FunctionCallExpression(TraversalStepType.SIDE_EFFECT, result, EMIT_METHOD, emitExpr);
        return result;

    }

    @Override
    public GroovyExpression getLoopExpressionParent(GroovyExpression inputQry) {
        GroovyExpression curTraversal = getAnonymousTraversalStartExpression();
        return curTraversal;
    }

    private IdentifierExpression getAnonymousTraversalStartExpression() {
        return new IdentifierExpression(TraversalStepType.START, "__");
    }

    @Override
    public GroovyExpression generateSelectExpression(GroovyExpression parent, List<LiteralExpression> sourceNames,
            List<GroovyExpression> srcExprs) {
        FunctionCallExpression result = new FunctionCallExpression(TraversalStepType.MAP_TO_VALUE, parent, SELECT_METHOD, sourceNames);

        for (GroovyExpression expr : srcExprs) {
            GroovyExpression closure = new ClosureExpression(expr);
            GroovyExpression castClosure = new TypeCoersionExpression(closure, FUNCTION_CLASS);
            result = new FunctionCallExpression(TraversalStepType.SIDE_EFFECT, result, BY_METHOD, castClosure);
        }
        return result;
    }

    @Override
    public GroovyExpression generateFieldExpression(GroovyExpression parent, FieldInfo fInfo,
            String propertyName, boolean inSelect)  {

        AttributeInfo attrInfo = fInfo.attrInfo();
        IDataType attrType = attrInfo.dataType();
        GroovyExpression propertyNameExpr = new LiteralExpression(propertyName);
        //Whether it is the user or shared graph does not matter here, since we're
        //just getting the conversion expression.  Ideally that would be moved someplace else.
        AtlasGraph graph = AtlasGraphProvider.getGraphInstance();
        if (inSelect) {

            GroovyExpression expr = new FunctionCallExpression(parent, PROPERTY_METHOD, propertyNameExpr);
            expr = new FunctionCallExpression(expr, OR_ELSE_METHOD, LiteralExpression.NULL);
            return graph.generatePersisentToLogicalConversionExpression(expr, attrType);
        } else {

            GroovyExpression unmapped = new FunctionCallExpression(TraversalStepType.FLAT_MAP_TO_VALUES, parent, VALUES_METHOD, propertyNameExpr);
            if (graph.isPropertyValueConversionNeeded(attrType)) {
                GroovyExpression toConvert = new FunctionCallExpression(getItVariable(), GET_METHOD);

                GroovyExpression conversionFunction = graph.generatePersisentToLogicalConversionExpression(toConvert,
                        attrType);
                return new FunctionCallExpression(TraversalStepType.MAP_TO_VALUE, unmapped, MAP_METHOD, new ClosureExpression(conversionFunction));
            } else {
                return unmapped;
            }

        }
    }

    private ComparisonOperator getGroovyOperator(String symbol) throws AtlasException {

        String toFind = symbol;
        if (toFind.equals("=")) {
            toFind = "==";
        }
        return ComparisonOperator.lookup(toFind);
    }

    private String getComparisonFunction(String op) throws AtlasException {

        if (op.equals("=")) {
            return "eq";
        }
        if (op.equals("!=")) {
            return "neq";
        }
        if (op.equals(">")) {
            return "gt";
        }
        if (op.equals(">=")) {
            return "gte";
        }
        if (op.equals("<")) {
            return "lt";
        }
        if (op.equals("<=")) {
            return "lte";
        }
        throw new AtlasException("Comparison operator " + op + " not supported in Gremlin");
    }

    @Override
    public GroovyExpression generateHasExpression(GraphPersistenceStrategies s, GroovyExpression parent,
            String propertyName, String symbol, GroovyExpression requiredValue, FieldInfo fInfo) throws AtlasException {

        AttributeInfo attrInfo = fInfo.attrInfo();
        IDataType attrType = attrInfo.dataType();
        GroovyExpression propertNameExpr = new LiteralExpression(propertyName);
        if (s.isPropertyValueConversionNeeded(attrType)) {
            // for some types, the logical value cannot be stored directly in
            // the underlying graph,
            // and conversion logic is needed to convert the persistent form of
            // the value
            // to the actual value. In cases like this, we generate a conversion
            // expression to
            // do this conversion and use the filter step to perform the
            // comparsion in the gremlin query
            GroovyExpression itExpr = getItVariable();
            GroovyExpression vertexExpr = new CastExpression(new FunctionCallExpression(itExpr, GET_METHOD), VERTEX_CLASS);
            GroovyExpression propertyValueExpr = new FunctionCallExpression(vertexExpr, VALUE_METHOD, propertNameExpr);
            GroovyExpression conversionExpr = s.generatePersisentToLogicalConversionExpression(propertyValueExpr,
                    attrType);

            GroovyExpression propertyIsPresentExpression = new FunctionCallExpression(
                    new FunctionCallExpression(vertexExpr, PROPERTY_METHOD, propertNameExpr), IS_PRESENT_METHOD);
            GroovyExpression valueMatchesExpr = new ComparisonExpression(conversionExpr, getGroovyOperator(symbol),
                    requiredValue);

            GroovyExpression filterCondition = new LogicalExpression(propertyIsPresentExpression, LogicalOperator.AND,
                    valueMatchesExpr);

            GroovyExpression filterFunction = new ClosureExpression(filterCondition);
            return new FunctionCallExpression(TraversalStepType.FILTER, parent, FILTER_METHOD, filterFunction);
        } else {
            GroovyExpression valueMatches = new FunctionCallExpression(getComparisonFunction(symbol), requiredValue);
            return new FunctionCallExpression(TraversalStepType.FILTER, parent, HAS_METHOD, propertNameExpr, valueMatches);
        }
    }

    @Override
    public GroovyExpression generateLikeExpressionUsingFilter(GroovyExpression parent, String propertyName, GroovyExpression propertyValue) throws AtlasException {
        GroovyExpression vertexExpr        = new FunctionCallExpression(getItVariable(), GET_METHOD);
        GroovyExpression propertyValueExpr = new FunctionCallExpression(vertexExpr, VALUE_METHOD, new LiteralExpression(propertyName));
        GroovyExpression matchesExpr       = new FunctionCallExpression(propertyValueExpr, MATCHES, escapePropertyValue(propertyValue));
        GroovyExpression closureExpr       = new ClosureExpression(matchesExpr);

        return new FunctionCallExpression(TraversalStepType.FILTER, parent, FILTER_METHOD, closureExpr);
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

    @Override
    protected GroovyExpression initialExpression(GroovyExpression varExpr, GraphPersistenceStrategies s) {

        // this bit of groovy magic converts the set of vertices in varName into
        // a String containing the ids of all the vertices. This becomes the
        // argument
        // to g.V(). This is needed because Gremlin 3 does not support
        // _()
        // s"g.V(${varName}.collect{it.id()} as String[])"

        GroovyExpression gExpr = getGraphExpression();
        GroovyExpression varRefExpr = new TypeCoersionExpression(varExpr, OBJECT_ARRAY_CLASS);
        GroovyExpression matchingVerticesExpr = new FunctionCallExpression(TraversalStepType.START, gExpr, V_METHOD, varRefExpr);
        GroovyExpression isEmpty  = new FunctionCallExpression(varExpr, "isEmpty");
        GroovyExpression emptyGraph = getEmptyTraversalExpression();

        GroovyExpression expr = new TernaryOperatorExpression(isEmpty, emptyGraph, matchingVerticesExpr);

        return s.addInitialQueryCondition(expr);
    }

    private GroovyExpression getEmptyTraversalExpression() {
        GroovyExpression emptyGraph = new FunctionCallExpression(TraversalStepType.START, getGraphExpression(), V_METHOD, EMPTY_STRING_EXPRESSION);
        return emptyGraph;
    }

    @Override
    public GroovyExpression generateRangeExpression(GroovyExpression parent, int startIndex, int endIndex) {
        //treat as barrier step, since limits need to be applied globally (even though it
        //is technically a filter step)
        return new FunctionCallExpression(TraversalStepType.BARRIER, parent, RANGE_METHOD, new LiteralExpression(startIndex), new LiteralExpression(endIndex));
    }

    @Override
    public boolean isRangeExpression(GroovyExpression expr) {

        return (expr instanceof FunctionCallExpression && ((FunctionCallExpression)expr).getFunctionName().equals(RANGE_METHOD));
    }

    @Override
    public int[] getRangeParameters(AbstractFunctionExpression expr) {

        if (isRangeExpression(expr)) {
            FunctionCallExpression rangeExpression = (FunctionCallExpression) expr;
            List<GroovyExpression> arguments = rangeExpression.getArguments();
            int startIndex = (int)((LiteralExpression)arguments.get(0)).getValue();
            int endIndex = (int)((LiteralExpression)arguments.get(1)).getValue();
            return new int[]{startIndex, endIndex};
        }
        else {
            return null;
        }
    }

    @Override
    public void setRangeParameters(GroovyExpression expr, int startIndex, int endIndex) {

        if (isRangeExpression(expr)) {
            FunctionCallExpression rangeExpression = (FunctionCallExpression) expr;
            rangeExpression.setArgument(0, new LiteralExpression(Integer.valueOf(startIndex)));
            rangeExpression.setArgument(1, new LiteralExpression(Integer.valueOf(endIndex)));
        }
        else {
            throw new IllegalArgumentException(expr + " is not a valid range expression");
        }
    }

    @Override
    public List<GroovyExpression> getOrderFieldParents() {

        List<GroovyExpression> result = new ArrayList<>(1);
        result.add(null);
        return result;
    }

    @Override
    public GroovyExpression generateOrderByExpression(GroovyExpression parent, List<GroovyExpression> translatedOrderBy,
            boolean isAscending) {

        GroovyExpression orderByExpr = translatedOrderBy.get(0);
        GroovyExpression orderByClosure = new ClosureExpression(orderByExpr);
        GroovyExpression orderByClause = new TypeCoersionExpression(orderByClosure, FUNCTION_CLASS);

        GroovyExpression aExpr = new IdentifierExpression("a");
        GroovyExpression bExpr = new IdentifierExpression("b");

        GroovyExpression aCompExpr = new FunctionCallExpression(new FunctionCallExpression(aExpr, TO_STRING_METHOD), TO_LOWER_CASE_METHOD);
        GroovyExpression bCompExpr = new FunctionCallExpression(new FunctionCallExpression(bExpr, TO_STRING_METHOD), TO_LOWER_CASE_METHOD);

        GroovyExpression comparisonExpr = null;
        if (isAscending) {
            comparisonExpr = new ComparisonOperatorExpression(aCompExpr, bCompExpr);
        } else {
            comparisonExpr = new ComparisonOperatorExpression(bCompExpr, aCompExpr);
        }
        ClosureExpression comparisonFunction = new ClosureExpression(comparisonExpr, "a", "b");
        FunctionCallExpression orderCall = new FunctionCallExpression(TraversalStepType.BARRIER, parent, ORDER_METHOD);
        return new FunctionCallExpression(TraversalStepType.SIDE_EFFECT, orderCall, BY_METHOD, orderByClause, comparisonFunction);
    }

    @Override
    public GroovyExpression getAnonymousTraversalExpression() {
        return null;
    }

    @Override
    public GroovyExpression getFieldInSelect() {
        // this logic is needed to remove extra results from
        // what is emitted by repeat loops. Technically
        // for queries that don't have a loop in them we could just use "it"
        // the reason for this is that in repeat loops with an alias,
        // although the alias gets set to the right value, for some
        // reason the select actually includes all vertices that were traversed
        // through in the loop. In these cases, we only want the last vertex
        // traversed in the loop to be selected. The logic here handles that
        // case by converting the result to a list and just selecting the
        // last item from it.

        GroovyExpression itExpr = getItVariable();
        GroovyExpression expr1 = new TypeCoersionExpression(itExpr, VERTEX_ARRAY_CLASS);
        GroovyExpression expr2 = new TypeCoersionExpression(expr1, VERTEX_LIST_CLASS);

        return new FunctionCallExpression(expr2, LAST_METHOD);
    }

    @Override
    public GroovyExpression generateGroupByExpression(GroovyExpression parent, GroovyExpression groupByExpression,
            GroovyExpression aggregationFunction) {

        GroovyExpression result = new FunctionCallExpression(TraversalStepType.BARRIER, parent, "group");
        GroovyExpression groupByClosureExpr = new TypeCoersionExpression(new ClosureExpression(groupByExpression), "Function");
        result = new FunctionCallExpression(TraversalStepType.SIDE_EFFECT, result, "by", groupByClosureExpr);
        result = new FunctionCallExpression(TraversalStepType.END, result, "toList");

        GroovyExpression mapValuesClosure = new ClosureExpression(new FunctionCallExpression(new CastExpression(getItVariable(), "Map"), "values"));

        result = new FunctionCallExpression(result, "collect", mapValuesClosure);

        //when we call Map.values(), we end up with an extra list around the result.  We remove this by calling toList().get(0).  This
        //leaves us with a list of lists containing the vertices that match each group.  We then apply the aggregation functions
        //specified in the select list to each of these inner lists.

        result = new FunctionCallExpression(result ,"toList");
        result = new FunctionCallExpression(result, "get", new LiteralExpression(0));

        GroovyExpression aggregrationFunctionClosure = new ClosureExpression(aggregationFunction);
        result = new FunctionCallExpression(result, "collect", aggregrationFunctionClosure);
        return result;
    }

    @Override
    public GroovyExpression generateSeededTraversalExpresssion(boolean isMap, GroovyExpression valueCollection) {
        GroovyExpression coersedExpression = new TypeCoersionExpression(valueCollection, isMap ? "Map[]" : "Vertex[]");
        if(isMap) {

            return new FunctionCallExpression(TraversalStepType.START, "__", coersedExpression);
        }
        else {
            //We cannot always use an anonymous traversal because that breaks repeat steps
            return new FunctionCallExpression(TraversalStepType.START, getEmptyTraversalExpression(), "inject", coersedExpression);
        }
    }

    @Override
    public GroovyExpression getGroupBySelectFieldParent() {
        return null;
    }

    @Override
    public String getTraversalExpressionClass() {
        return "GraphTraversal";
    }

    @Override
    public boolean isSelectGeneratesMap(int aliasCount) {
        //in Gremlin 3, you only get a map if there is more than 1 alias.
        return aliasCount > 1;
    }

    @Override
    public GroovyExpression generateMapExpression(GroovyExpression parent, ClosureExpression closureExpression) {
        return new FunctionCallExpression(TraversalStepType.MAP_TO_ELEMENT, parent, "map", closureExpression);
    }

    @Override
    public GroovyExpression generateGetSelectedValueExpression(LiteralExpression key,
            GroovyExpression rowMapExpr) {
        rowMapExpr = new CastExpression(rowMapExpr, "Map");
        GroovyExpression getExpr = new FunctionCallExpression(rowMapExpr, "get", key);
        return getExpr;
    }

    @Override
    public GroovyExpression getCurrentTraverserObject(GroovyExpression traverser) {
        return new FunctionCallExpression(traverser, "get");
    }

    public List<String> getAliasesRequiredByExpression(GroovyExpression expr) {
        return Collections.emptyList();
    }

    @Override
    public boolean isRepeatExpression(GroovyExpression expr) {
        if(!(expr instanceof FunctionCallExpression)) {
            return false;
        }
        return ((FunctionCallExpression)expr).getFunctionName().equals(REPEAT_METHOD);
    }
}
