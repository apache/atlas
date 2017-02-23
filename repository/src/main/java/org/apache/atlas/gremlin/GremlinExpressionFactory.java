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
 * distributed under the License is distributed on an "AS_METHOD IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.gremlin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.atlas.AtlasException;
import org.apache.atlas.groovy.AbstractFunctionExpression;
import org.apache.atlas.groovy.ArithmeticExpression;
import org.apache.atlas.groovy.ArithmeticExpression.ArithmeticOperator;
import org.apache.atlas.groovy.CastExpression;
import org.apache.atlas.groovy.ClosureExpression;
import org.apache.atlas.groovy.FieldExpression;
import org.apache.atlas.groovy.FunctionCallExpression;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.IdentifierExpression;
import org.apache.atlas.groovy.ListExpression;
import org.apache.atlas.groovy.LiteralExpression;
import org.apache.atlas.groovy.TraversalStepType;
import org.apache.atlas.groovy.TypeCoersionExpression;
import org.apache.atlas.groovy.VariableAssignmentExpression;
import org.apache.atlas.query.GraphPersistenceStrategies;
import org.apache.atlas.query.IntSequence;
import org.apache.atlas.query.TypeUtils.FieldInfo;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.GremlinVersion;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.cache.TypeCache.TYPE_FILTER;
import org.apache.atlas.util.AtlasRepositoryConfiguration;

import com.google.common.collect.ImmutableList;

/**
 * Factory to generate Groovy expressions representing Gremlin syntax that that
 * are independent of the specific version of Gremlin that is being used.
 *
 */
public abstract class GremlinExpressionFactory {

    private static final String G_VARIABLE = "g";
    private static final String IT_VARIABLE = "it";

    protected static final String SET_CLASS = "Set";


    private static final String OBJECT_FIELD = "object";

    protected static final String V_METHOD = "V";
    protected static final String FILTER_METHOD = "filter";
    private static final String PATH_METHOD = "path";
    private static final String AS_METHOD = "as";
    private static final String IN_OPERATOR = "in";
    protected static final String HAS_METHOD = "has";
    protected static final String TO_LOWER_CASE_METHOD = "toLowerCase";
    protected static final String SELECT_METHOD = "select";
    protected static final String ORDER_METHOD = "order";
    protected static final String FILL_METHOD = "fill";

    public static final GremlinExpressionFactory INSTANCE = AtlasGraphProvider.getGraphInstance()
            .getSupportedGremlinVersion() == GremlinVersion.THREE ? new Gremlin3ExpressionFactory()
                    : new Gremlin2ExpressionFactory();

    /**
     * Returns the unqualified name of the class used in this version of gremlin to
     * represent Gremlin queries as they are being generated.
     * @return
     */
    public abstract String getTraversalExpressionClass();

    /**
     * Gets the expression to use as the parent when translating the loop
     * expression in a loop
     *
     * @param inputQry
     *            the
     * @return
     */
    public abstract GroovyExpression getLoopExpressionParent(GroovyExpression inputQry);

    /**
     * Generates a loop expression.
     *
     * @param parent
     *            the parent of the loop expression
     * @param emitExpr
     *            Expression with the value that should be emitted by the loop
     *            expression.
     * @param loopExpr
     *            the query expression that is being executed repeatedly
     *            executed in a loop
     * @param alias
     *            The alias of the expression being looped over
     * @param times
     *            the number of times to repeat, or null if a times condition
     *            should not be used.
     * @return
     */
    public abstract GroovyExpression generateLoopExpression(GroovyExpression parent, GraphPersistenceStrategies s, IDataType dataType,
            GroovyExpression loopExpr, String alias, Integer times);


    /**
     * Generates a logical (and/or) expression with the given operands.
     * @param parent
     * @param operator
     * @param operands
     * @return
     */
    public abstract GroovyExpression generateLogicalExpression(GroovyExpression parent, String operator,
            List<GroovyExpression> operands);

    /**
     * Generates a back reference expression that refers to the given alias.
     *
     * @param parent
     * @param inSelect
     * @param alias
     * @return
     */
    public abstract GroovyExpression generateBackReferenceExpression(GroovyExpression parent, boolean inSelect,
            String alias);

    /**
     * Generates a select expression
     *
     * @param parent
     * @param sourceNames
     *            the names of the select fields
     * @param srcExprs
     *            the corresponding values to return
     * @return
     */
    public abstract GroovyExpression generateSelectExpression(GroovyExpression parent,
            List<LiteralExpression> sourceNames, List<GroovyExpression> srcExprs);

    /**
     * Generates a an expression that gets the value of the given property from the
     * vertex presented by the parent.
     *
     * @param parent
     * @param fInfo
     * @param propertyName
     * @param inSelect
     * @return
     */
    public abstract GroovyExpression generateFieldExpression(GroovyExpression parent, FieldInfo fInfo,
            String propertyName, boolean inSelect);

    /**
     * Generates a has expression that checks whether the vertices match a specific condition
     *
     * @param s
     * @param parent the object that we should call apply the "has" condition to.
     * @param propertyName the name of the property whose value we are comparing
     * @param symbol comparsion operator symbol ('=','<', etc.)
     * @param requiredValue the value to compare against
     * @param fInfo info about the field whose value we are checking
     * @return
     * @throws AtlasException
     */
    public abstract GroovyExpression generateHasExpression(GraphPersistenceStrategies s, GroovyExpression parent,
            String propertyName, String symbol, GroovyExpression requiredValue, FieldInfo fInfo) throws AtlasException;

    /**
     * Generates a range expression
     *
     * @param parent
     * @param startIndex
     * @param endIndex
     * @return
     */
    public abstract GroovyExpression generateRangeExpression(GroovyExpression parent, int startIndex, int endIndex);

    /**
     * Determines if the specified expression is a range method call.
     *
     * @param expr
     * @return
     */
    public abstract boolean isRangeExpression(GroovyExpression expr);

    /**
     * Set the start index and end index of a range expression
     *
     * @param expr
     * @param startIndex
     * @param endIndex
     */
    public abstract void setRangeParameters(GroovyExpression expr, int startIndex, int endIndex);

    /**
     * If the specified function expression is a range expression, returns the start and end index parameters
     * otherwise returns null.
     *
     * @param expr
     * @return int array with two elements - element 0 is start index, element 1 is end index
     */
    public abstract int[] getRangeParameters(AbstractFunctionExpression expr);

    /**
     * Generates an order by expression
     *
     * @param parent
     * @param translatedOrderBy
     * @param isAscending
     * @return
     */
    public abstract GroovyExpression generateOrderByExpression(GroovyExpression parent,
            List<GroovyExpression> translatedOrderBy, boolean isAscending);

    /**
     * Determines if specified expression is an order method call
     *
     * @param expr
     * @return
     */
    public boolean isOrderExpression(GroovyExpression expr) {
        if (expr instanceof FunctionCallExpression) {
            FunctionCallExpression functionCallExpression = (FunctionCallExpression) expr;
            if (functionCallExpression.getFunctionName().equals(ORDER_METHOD)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the Groovy expressions that should be used as the parents when
     * translating an order by expression. This is needed because Gremlin 2 and
     * 3 handle order by expressions very differently.
     *
     */
    public abstract List<GroovyExpression> getOrderFieldParents();

    /**
     * Returns the expression that represents an anonymous graph traversal.
     *
     * @return
     */
    public abstract GroovyExpression getAnonymousTraversalExpression();

    public boolean isLeafAnonymousTraversalExpression(GroovyExpression expr) {
        if(!(expr instanceof FunctionCallExpression)) {
            return false;
        }
        FunctionCallExpression functionCallExpr = (FunctionCallExpression)expr;
        if(functionCallExpr.getCaller() != null) {
            return false;
        }
        return functionCallExpr.getFunctionName().equals("_") & functionCallExpr.getArguments().size() == 0;
    }

    /**
     * Returns an expression representing
     *
     * @return
     */
    public abstract GroovyExpression getFieldInSelect();

    /**
     * Generates the expression the serves as the root of the Gremlin query.
     * @param varExpr variable containing the vertices to traverse
     * @return
     */
    protected abstract GroovyExpression initialExpression(GroovyExpression varExpr, GraphPersistenceStrategies s);


    /**
     * Generates an expression that tests whether the vertex represented by the 'toTest'
     * expression represents an instance of the specified type, checking both the type
     * and super type names.
     *
     * @param s
     * @param typeName
     * @param itRef
     * @return
     */
    protected abstract GroovyExpression typeTestExpression(GraphPersistenceStrategies s, String typeName,
            GroovyExpression vertexExpr);

    /**
    /**
     * Generates a sequence of groovy expressions that filter the vertices to only
     * those that match the specified type.  If GraphPersistenceStrategies.collectTypeInstancesIntoVar()
     * is set and the gremlin optimizer is disabled, the vertices are put into a variable whose name is generated
     * from the specified IntSequence.  The last item in the result will be a graph traversal restricted to only
     * the matching vertices.
     */
    public List<GroovyExpression> generateTypeTestExpression(GraphPersistenceStrategies s, GroovyExpression parent,
                                                             String typeName, IntSequence intSeq) throws AtlasException {

        if(AtlasRepositoryConfiguration.isGremlinOptimizerEnabled()) {
            GroovyExpression superTypeAttributeNameExpr = new LiteralExpression(s.superTypeAttributeName());
            GroovyExpression typeNameExpr = new LiteralExpression(typeName);
            GroovyExpression superTypeMatchesExpr = new FunctionCallExpression(TraversalStepType.FILTER, HAS_METHOD, superTypeAttributeNameExpr,
                    typeNameExpr);

            GroovyExpression typeAttributeNameExpr = new LiteralExpression(s.typeAttributeName());

            GroovyExpression typeMatchesExpr = new FunctionCallExpression(TraversalStepType.FILTER, HAS_METHOD, typeAttributeNameExpr,
                    typeNameExpr);
            GroovyExpression result = new FunctionCallExpression(TraversalStepType.FILTER, parent, "or", typeMatchesExpr, superTypeMatchesExpr);
            return Collections.singletonList(result);
        }
        else {
            if (s.filterBySubTypes()) {
                return typeTestExpressionUsingInFilter(s, parent, typeName);
            } else if (s.collectTypeInstancesIntoVar()) {
                return typeTestExpressionMultiStep(s, typeName, intSeq);
            } else {
                return typeTestExpressionUsingFilter(s, parent, typeName);
            }
        }
    }

    private List<GroovyExpression> typeTestExpressionUsingInFilter(GraphPersistenceStrategies s, GroovyExpression parent,
                                                                   final String typeName) throws AtlasException {
        List<GroovyExpression> typeNames = new ArrayList<>();
        typeNames.add(new LiteralExpression(typeName));

        Map<TYPE_FILTER, String> filters = new HashMap<TYPE_FILTER, String>() {{
            put(TYPE_FILTER.SUPERTYPE, typeName);
        }};

        ImmutableList<String> subTypes = TypeSystem.getInstance().getTypeNames(filters);

        if (!subTypes.isEmpty()) {
            for (String subType : subTypes) {
                typeNames.add(new LiteralExpression(subType));
            }
        }

        GroovyExpression inFilterExpr = generateHasExpression(s, parent, s.typeAttributeName(), IN_OPERATOR,
                                                              new ListExpression(typeNames), null);

        return Collections.singletonList(inFilterExpr);
    }

    private List<GroovyExpression> typeTestExpressionMultiStep(GraphPersistenceStrategies s, String typeName,
            IntSequence intSeq) {

        String varName = "_var_" + intSeq.next();
        GroovyExpression varExpr = new IdentifierExpression(varName);
        List<GroovyExpression> result = new ArrayList<>();

        result.add(newSetVar(varName));
        result.add(fillVarWithTypeInstances(s, typeName, varName));
        result.add(fillVarWithSubTypeInstances(s, typeName, varName));
        result.add(initialExpression(varExpr, s));

        return result;
    }

    private GroovyExpression newSetVar(String varName) {
        GroovyExpression castExpr = new TypeCoersionExpression(new ListExpression(), SET_CLASS);
        return new VariableAssignmentExpression(varName, castExpr);
    }

    private GroovyExpression fillVarWithTypeInstances(GraphPersistenceStrategies s, String typeName, String fillVar) {
        GroovyExpression graphExpr = getAllVerticesExpr();
        GroovyExpression typeAttributeNameExpr = new LiteralExpression(s.typeAttributeName());
        GroovyExpression typeNameExpr = new LiteralExpression(typeName);
        GroovyExpression hasExpr = new FunctionCallExpression(graphExpr, HAS_METHOD, typeAttributeNameExpr, typeNameExpr);
        GroovyExpression fillExpr = new FunctionCallExpression(hasExpr, FILL_METHOD, new IdentifierExpression(fillVar));
        return fillExpr;
    }

    private GroovyExpression fillVarWithSubTypeInstances(GraphPersistenceStrategies s, String typeName,
            String fillVar) {
        GroovyExpression graphExpr = getAllVerticesExpr();
        GroovyExpression superTypeAttributeNameExpr = new LiteralExpression(s.superTypeAttributeName());
        GroovyExpression typeNameExpr = new LiteralExpression(typeName);
        GroovyExpression hasExpr = new FunctionCallExpression(graphExpr, HAS_METHOD, superTypeAttributeNameExpr, typeNameExpr);
        GroovyExpression fillExpr = new FunctionCallExpression(hasExpr, FILL_METHOD, new IdentifierExpression(fillVar));
        return fillExpr;
    }


    private List<GroovyExpression> typeTestExpressionUsingFilter(GraphPersistenceStrategies s, GroovyExpression parent,
            String typeName) {
        GroovyExpression itExpr = getItVariable();
        GroovyExpression typeTestExpr = typeTestExpression(s, typeName, itExpr);
        GroovyExpression closureExpr = new ClosureExpression(typeTestExpr);
        GroovyExpression filterExpr = new FunctionCallExpression(parent, FILTER_METHOD, closureExpr);
        return Collections.singletonList(filterExpr);
    }

    /**
     * Generates an expression which checks whether the vertices in the query have
     * a field with the given name.
     *
     * @param parent
     * @param fieldName
     * @return
     */
    public GroovyExpression generateUnaryHasExpression(GroovyExpression parent, String fieldName) {
        return new FunctionCallExpression(TraversalStepType.FILTER, parent, HAS_METHOD, new LiteralExpression(fieldName));
    }

    /**
     * Generates a path expression
     *
     * @param parent
     * @return
     */
    public GroovyExpression generatePathExpression(GroovyExpression parent) {
        return new FunctionCallExpression(TraversalStepType.MAP_TO_VALUE, parent, PATH_METHOD);
    }

    /**
     * Generates the emit expression used in loop expressions.
     * @param s
     * @param dataType
     * @return
     */
    protected GroovyExpression generateLoopEmitExpression(GraphPersistenceStrategies s, IDataType dataType) {
        return typeTestExpression(s, dataType.getName(), getCurrentObjectExpression());
    }

    /**
     * Generates an alias expression
     *
     * @param parent
     * @param alias
     * @return
     */
    public GroovyExpression generateAliasExpression(GroovyExpression parent, String alias) {
        return new FunctionCallExpression(TraversalStepType.SIDE_EFFECT, parent, AS_METHOD, new LiteralExpression(alias));
    }

    /**
     * Generates an expression that gets the vertices adjacent to the vertex in 'parent'
     * in the specified direction.
     *
     * @param parent
     * @param dir
     * @return
     */
    public GroovyExpression generateAdjacentVerticesExpression(GroovyExpression parent, AtlasEdgeDirection dir) {
        return new FunctionCallExpression(TraversalStepType.FLAT_MAP_TO_ELEMENTS, parent, getGremlinFunctionName(dir));
    }

    private String getGremlinFunctionName(AtlasEdgeDirection dir) {
        switch(dir) {
        case IN:
            return "in";
        case OUT:
            return "out";
        case BOTH:
            return "both";
        default:
            throw new RuntimeException("Unknown Atlas Edge Direction: " + dir);
        }
    }

    /**
     * Generates an expression that gets the vertices adjacent to the vertex in 'parent'
     * in the specified direction, following only edges with the given label.
     *
     * @param parent
     * @param dir
     * @return
     */
    public GroovyExpression generateAdjacentVerticesExpression(GroovyExpression parent, AtlasEdgeDirection dir,
            String label) {
        return new FunctionCallExpression(TraversalStepType.FLAT_MAP_TO_ELEMENTS, parent, getGremlinFunctionName(dir), new LiteralExpression(label));
    }

    /**
     * Generates an arithmetic expression, e.g. a + b
     *
     */
    public GroovyExpression generateArithmeticExpression(GroovyExpression left, String operator,
            GroovyExpression right) throws AtlasException {
        ArithmeticOperator op = ArithmeticOperator.lookup(operator);
        return new ArithmeticExpression(left, op, right);
    }

    public abstract GroovyExpression generateGroupByExpression(GroovyExpression parent, GroovyExpression groupByExpression, GroovyExpression aggregationFunction);

    protected GroovyExpression getItVariable() {
        return new IdentifierExpression(IT_VARIABLE);
    }

    protected GroovyExpression getAllVerticesExpr() {
        GroovyExpression gExpr = getGraphExpression();
        return new FunctionCallExpression(TraversalStepType.START, gExpr, V_METHOD);
    }

    protected IdentifierExpression getGraphExpression() {
        return new IdentifierExpression(TraversalStepType.SOURCE, G_VARIABLE);
    }


    protected GroovyExpression getCurrentObjectExpression() {
        return new FieldExpression(getItVariable(), OBJECT_FIELD);
    }

    //assumes cast already performed
    public GroovyExpression generateCountExpression(GroovyExpression itExpr) {
        GroovyExpression collectionExpr = new CastExpression(itExpr,"Collection");
        return new FunctionCallExpression(collectionExpr, "size");
    }

    public GroovyExpression generateMinExpression(GroovyExpression itExpr, GroovyExpression mapFunction) {
        return getAggregrationExpression(itExpr, mapFunction, "min");
    }

    public GroovyExpression generateMaxExpression(GroovyExpression itExpr, GroovyExpression mapFunction) {
        return getAggregrationExpression(itExpr, mapFunction, "max");
    }

    public GroovyExpression generateSumExpression(GroovyExpression itExpr, GroovyExpression mapFunction) {
        return getAggregrationExpression(itExpr, mapFunction, "sum");
    }

    private GroovyExpression getAggregrationExpression(GroovyExpression itExpr,
            GroovyExpression mapFunction, String functionName) {
        GroovyExpression collectionExpr = new CastExpression(itExpr,"Collection");
        ClosureExpression collectFunction = new ClosureExpression(mapFunction);
        GroovyExpression transformedList = new FunctionCallExpression(collectionExpr, "collect", collectFunction);
        return new FunctionCallExpression(transformedList, functionName);
    }

    public GroovyExpression getClosureArgumentValue() {
        return getItVariable();
    }

    /**
     * Specifies the parent to use when translating the select list in
     * a group by statement.
     *
     * @return
     */
    public abstract GroovyExpression getGroupBySelectFieldParent();

    public GroovyExpression generateFillExpression(GroovyExpression parent, GroovyExpression variable) {
        return new FunctionCallExpression(TraversalStepType.END,parent , "fill", variable);
    }

    /**
     * Generates an anonymous graph traversal initialized with the specified value.  In Gremlin 3, we need
     * to use  a different syntax for this when the object is a map, so that information needs to be provided
     * to this method so that the correct syntax is used.
     *
     * @param isMap true if the value contains Map instances, false if it contains Vertex instances
     * @param valueCollection the source objects to start the traversal from.
     */
    public abstract GroovyExpression generateSeededTraversalExpresssion(boolean isMap, GroovyExpression valueCollection);

    /**
     * Returns the current value of the traverser.  This is used when generating closure expressions that
     * need to operate on the current value in the graph graversal.
     *
     * @param traverser
     * @return
     */
    public abstract GroovyExpression getCurrentTraverserObject(GroovyExpression traverser);

    /**
     * Generates an expression that transforms the current value of the traverser by
     * applying the function specified
     *
     * @param parent
     * @param closureExpression
     * @return
     */
    public abstract GroovyExpression generateMapExpression(GroovyExpression parent, ClosureExpression closureExpression);

    /**
     * Returns whether a select statement generates a map (or Gremlin 2 "Row") when it contains the specified
     * number of aliases.
     *
     */
    public abstract boolean isSelectGeneratesMap(int aliasCount);

    /**
     * Generates an expression to get the value of the value from the row map
     * generated by select() with the specified key.
     *
     */
    public abstract GroovyExpression generateGetSelectedValueExpression(LiteralExpression key,
            GroovyExpression rowMapExpr);

    public GroovyExpression removeExtraMapFromPathInResult(GroovyExpression parent) {
        GroovyExpression listItem = getItVariable();
        GroovyExpression tailExpr = new FunctionCallExpression(listItem, "tail");
        return new FunctionCallExpression(parent, "collect", new ClosureExpression(tailExpr));

    }

    /**
     * Generates a toList expression to execute the gremlin query and
     * store the result in a new list.
     *
     * @param expr
     * @return
     */
    public GroovyExpression generateToListExpression(GroovyExpression expr) {
        return new FunctionCallExpression(TraversalStepType.END, expr, "toList");
    }

    /**
     * Finds aliases that absolutely must be brought along with this expression into
     * the output expression and cannot just be recreated there.  For example, in the
     * Gremlin 2 loop expression, the loop semantics break of the alias is simply recreated
     * in the output expression.
     * @param expr
     * @return
     */
    public abstract List<String> getAliasesRequiredByExpression(GroovyExpression expr);


    /**
     * Checks if the given expression is an alias expression, and if so
     * returns the alias from the expression.  Otherwise, null is
     * returned.
     */
    public String getAliasNameIfRelevant(GroovyExpression expr) {
        if(!(expr instanceof FunctionCallExpression)) {
            return null;
        }
        FunctionCallExpression fc = (FunctionCallExpression)expr;
        if(! fc.getFunctionName().equals(AS_METHOD)) {
            return null;
        }
       LiteralExpression aliasName =  (LiteralExpression)fc.getArguments().get(0);
       return aliasName.getValue().toString();

    }

    public abstract boolean isRepeatExpression(GroovyExpression expr);
}