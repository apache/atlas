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
package org.apache.atlas.repository.graph;

import org.apache.atlas.AtlasException;
import org.apache.atlas.gremlin.GremlinExpressionFactory;
import org.apache.atlas.gremlin.optimizer.GremlinQueryOptimizer;
import org.apache.atlas.gremlin.optimizer.RangeFinder;
import org.apache.atlas.groovy.AbstractFunctionExpression;
import org.apache.atlas.groovy.FunctionCallExpression;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.IdentifierExpression;
import org.apache.atlas.groovy.LiteralExpression;
import org.apache.atlas.groovy.TraversalStepType;
import org.apache.atlas.query.GraphPersistenceStrategies;
import org.apache.atlas.query.TypeUtils.FieldInfo;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.MetadataRepository;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.GremlinVersion;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public abstract class AbstractGremlinQueryOptimizerTest implements IAtlasGraphProvider {

    protected abstract GremlinExpressionFactory getFactory();

    private MetadataRepository repo;
    private final GraphPersistenceStrategies STRATEGY = mock(GraphPersistenceStrategies.class);

    @BeforeClass
    public void setUp() throws RepositoryException {
        GremlinQueryOptimizer.reset();
        GremlinQueryOptimizer.setExpressionFactory(getFactory());
        when(STRATEGY.typeAttributeName()).thenReturn(Constants.ENTITY_TYPE_PROPERTY_KEY);
        when(STRATEGY.superTypeAttributeName()).thenReturn(Constants.SUPER_TYPES_PROPERTY_KEY);
        repo = new GraphBackedMetadataRepository(new HardDeleteHandler(TypeSystem.getInstance()), this.get());
    }

    private FieldInfo getTestFieldInfo() throws AtlasException {
        AttributeDefinition def = new AttributeDefinition("foo", DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null);
        AttributeInfo attrInfo = new AttributeInfo(TypeSystem.getInstance(), def, null);
        return new FieldInfo(DataTypes.STRING_TYPE, attrInfo, null, null);
    }

    private GroovyExpression getVerticesExpression() {
        IdentifierExpression g = new IdentifierExpression("g");
        return new FunctionCallExpression(TraversalStepType.START, g, "V");
    }


    @Test
    public void testPullHasExpressionsOutOfAnd() throws AtlasException {

        GroovyExpression expr1 = makeOutExpression(null, "out1");
        GroovyExpression expr2 = makeOutExpression(null, "out2");
        GroovyExpression expr3 = makeHasExpression("prop1","Fred");
        GroovyExpression expr4 = makeHasExpression("prop2","George");
        GroovyExpression toOptimize = getFactory().generateLogicalExpression(getVerticesExpression(), "and", Arrays.asList(expr1, expr2, expr3, expr4));

        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), getExpectedGremlinForTestPullHasExpressionsOutOfHas());
    }

    protected abstract String getExpectedGremlinForTestPullHasExpressionsOutOfHas();


    @Test
    public void testOrGrouping() throws AtlasException {
        GroovyExpression expr1 = makeOutExpression(null, "out1");
        GroovyExpression expr2 = makeOutExpression(null, "out2");
        GroovyExpression expr3 = makeHasExpression("prop1","Fred");
        GroovyExpression expr4 = makeHasExpression("prop2","George");
        GroovyExpression toOptimize = getFactory().generateLogicalExpression(getVerticesExpression(), "or", Arrays.asList(expr1, expr2, expr3, expr4));

        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), getExpectedGremlinForTestOrGrouping());
    }

    protected abstract String getExpectedGremlinForTestOrGrouping();


    @Test
    public void testAndOfOrs() throws AtlasException {

        GroovyExpression or1Cond1 = makeHasExpression("p1","e1");
        GroovyExpression or1Cond2 = makeHasExpression("p2","e2");
        GroovyExpression or2Cond1 = makeHasExpression("p3","e3");
        GroovyExpression or2Cond2 = makeHasExpression("p4","e4");

        GroovyExpression or1 = getFactory().generateLogicalExpression(null, "or", Arrays.asList(or1Cond1, or1Cond2));
        GroovyExpression or2 = getFactory().generateLogicalExpression(null, "or", Arrays.asList(or2Cond1, or2Cond2));
        GroovyExpression toOptimize  = getFactory().generateLogicalExpression(getVerticesExpression(), "and", Arrays.asList(or1, or2));

        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), getExpectedGremlinForTestAndOfOrs());

    }

    protected abstract String getExpectedGremlinForTestAndOfOrs();

    @Test
    public void testAndWithMultiCallArguments() throws AtlasException {

        GroovyExpression cond1 = makeHasExpression("p1","e1");
        GroovyExpression cond2 = makeHasExpression(cond1, "p2","e2");
        GroovyExpression cond3 = makeHasExpression("p3","e3");
        GroovyExpression cond4 = makeHasExpression(cond3, "p4","e4");

        GroovyExpression toOptimize  = getFactory().generateLogicalExpression(getVerticesExpression(), "and", Arrays.asList(cond2, cond4));

        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), getExpectedGremlinForTestAndWithMultiCallArguments());
    }


    protected abstract String getExpectedGremlinForTestAndWithMultiCallArguments();

    @Test
    public void testOrOfAnds() throws AtlasException {

        GroovyExpression or1Cond1 = makeHasExpression("p1","e1");
        GroovyExpression or1Cond2 = makeHasExpression("p2","e2");
        GroovyExpression or2Cond1 = makeHasExpression("p3","e3");
        GroovyExpression or2Cond2 = makeHasExpression("p4","e4");

        GroovyExpression or1 = getFactory().generateLogicalExpression(null, "and", Arrays.asList(or1Cond1, or1Cond2));
        GroovyExpression or2 = getFactory().generateLogicalExpression(null, "and", Arrays.asList(or2Cond1, or2Cond2));
        GroovyExpression toOptimize  = getFactory().generateLogicalExpression(getVerticesExpression(), "or", Arrays.asList(or1, or2));

        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), getExpectedGremlinForTestOrOfAnds());
    }

    protected abstract String getExpectedGremlinForTestOrOfAnds();

    @Test
    public void testHasNotMovedToResult() throws AtlasException {
        GroovyExpression toOptimize = getVerticesExpression();
        GroovyExpression or1Cond1 = makeHasExpression("p1","e1");
        GroovyExpression or1Cond2 = makeHasExpression("p2","e2");

        toOptimize = getFactory().generateLogicalExpression(toOptimize, "or", Arrays.asList(or1Cond1, or1Cond2));
        toOptimize = makeHasExpression(toOptimize, "p3","e3");
        toOptimize = getFactory().generateAliasExpression(toOptimize, "_src");
        toOptimize = getFactory().generateSelectExpression(toOptimize, Collections.singletonList(new LiteralExpression("src1")), Collections.<GroovyExpression>singletonList(new IdentifierExpression("it")));
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(),
                getExpectedGremlinForTestHasNotMovedToResult());
    }

    protected abstract String getExpectedGremlinForTestHasNotMovedToResult();

    @Test
    public void testOptimizeLoopExpression() throws AtlasException {


        GroovyExpression input = getVerticesExpression();
        input = getFactory().generateTypeTestExpression(STRATEGY,  input, "DataSet", TestIntSequence.INSTANCE).get(0);
        input = makeHasExpression(input, "name","Fred");
        input = getFactory().generateAliasExpression(input, "label");


        GroovyExpression loopExpr = getFactory().getLoopExpressionParent(input);
        loopExpr = getFactory().generateAdjacentVerticesExpression(loopExpr, AtlasEdgeDirection.IN, "inputTables");
        loopExpr = getFactory().generateAdjacentVerticesExpression(loopExpr, AtlasEdgeDirection.OUT, "outputTables");
        GroovyExpression result = getFactory().generateLoopExpression(input, STRATEGY, DataTypes.STRING_TYPE, loopExpr, "label", null);
        result = getFactory().generateToListExpression(result);

        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(result);

        assertEquals(optimized.toString(), getExpectedGremlinForOptimizeLoopExpression());
    }

    protected abstract String getExpectedGremlinForOptimizeLoopExpression();

    @Test
    public void testLongStringEndingWithOr() throws AtlasException {
        GroovyExpression toOptimize = getVerticesExpression();
        toOptimize = makeHasExpression(toOptimize, "name","Fred");
        toOptimize = makeHasExpression(toOptimize, "age","13");
        toOptimize = makeOutExpression(toOptimize, "livesIn");
        toOptimize = makeHasExpression(toOptimize, "state","Massachusetts");

        GroovyExpression or1cond1 = makeHasExpression("p1", "e1");
        GroovyExpression or1cond2 = makeHasExpression("p2", "e2");
        toOptimize = getFactory().generateLogicalExpression(toOptimize, "or", Arrays.asList(or1cond1, or1cond2));

        GroovyExpression or2cond1 = makeHasExpression("p3", "e3");
        GroovyExpression or2cond2 = makeHasExpression("p4", "e4");
        toOptimize = getFactory().generateLogicalExpression(toOptimize, "or", Arrays.asList(or2cond1, or2cond2));
        toOptimize = makeHasExpression(toOptimize, "p5","e5");
        toOptimize = makeHasExpression(toOptimize, "p6","e6");
        GroovyExpression or3cond1 = makeHasExpression("p7", "e7");
        GroovyExpression or3cond2 = makeHasExpression("p8", "e8");
        toOptimize = getFactory().generateLogicalExpression(toOptimize, "or", Arrays.asList(or3cond1, or3cond2));


        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), getExpectedGremlinForTestLongStringEndingWithOr());
    }

    protected abstract String getExpectedGremlinForTestLongStringEndingWithOr();

    @Test
    public void testLongStringNotEndingWithOr() throws AtlasException {
        GroovyExpression toOptimize = getVerticesExpression();
        toOptimize = makeHasExpression(toOptimize, "name","Fred");
        toOptimize = makeHasExpression(toOptimize, "age","13");
        toOptimize = makeOutExpression(toOptimize, "livesIn");
        toOptimize = makeHasExpression(toOptimize, "state","Massachusetts");

        GroovyExpression or1cond1 = makeHasExpression("p1", "e1");
        GroovyExpression or1cond2 = makeHasExpression("p2", "e2");
        toOptimize = getFactory().generateLogicalExpression(toOptimize, "or", Arrays.asList(or1cond1, or1cond2));

        GroovyExpression or2cond1 = makeHasExpression("p3", "e3");
        GroovyExpression or2cond2 = makeHasExpression("p4", "e4");
        toOptimize = getFactory().generateLogicalExpression(toOptimize, "or", Arrays.asList(or2cond1, or2cond2));
        toOptimize = makeHasExpression(toOptimize, "p5","e5");
        toOptimize = makeHasExpression(toOptimize, "p6","e6");
        GroovyExpression or3cond1 = makeHasExpression("p7", "e7");
        GroovyExpression or3cond2 = makeHasExpression("p8", "e8");
        toOptimize = getFactory().generateLogicalExpression(toOptimize, "or", Arrays.asList(or3cond1, or3cond2));
        toOptimize = makeHasExpression(toOptimize, "p9","e9");

        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), getExpectedGremlinForTestLongStringNotEndingWithOr());
    }

    protected abstract String getExpectedGremlinForTestLongStringNotEndingWithOr();

    @Test
    public void testToListConversion() throws AtlasException {

        GroovyExpression expr1 = makeHasExpression("prop1","Fred");
        GroovyExpression expr2 = makeHasExpression("prop2","George");
        GroovyExpression toOptimize = getFactory().generateLogicalExpression(getVerticesExpression(), "or", Arrays.asList(expr1, expr2));
        toOptimize = new FunctionCallExpression(TraversalStepType.END, toOptimize,"toList");
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), getExpectedGremlinForTestToListConversion());
    }

    protected abstract String getExpectedGremlinForTestToListConversion();

    @Test
    public void testToListWithExtraStuff() throws AtlasException {

        GroovyExpression expr1 = makeHasExpression("prop1","Fred");
        GroovyExpression expr2 = makeHasExpression("prop2","George");
        GroovyExpression toOptimize = getFactory().generateLogicalExpression(getVerticesExpression(), "or", Arrays.asList(expr1, expr2));
        toOptimize = new FunctionCallExpression(TraversalStepType.END, toOptimize,"toList");
        toOptimize = new FunctionCallExpression(toOptimize,"size");
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), getExpectedGremlinForTestToListWithExtraStuff());

    }

    protected abstract String getExpectedGremlinForTestToListWithExtraStuff();

    public void testAddClosureWithExitExpressionDifferentFromExpr() throws AtlasException {

        GroovyExpression expr1 = makeHasExpression("prop1","Fred");
        GroovyExpression expr2 = makeHasExpression("prop2","George");
        GroovyExpression toOptimize = getFactory().generateLogicalExpression(getVerticesExpression(), "or", Arrays.asList(expr1, expr2));
        toOptimize = makeOutExpression(toOptimize, "knows");
        toOptimize = makeOutExpression(toOptimize, "livesIn");
        toOptimize = new FunctionCallExpression(TraversalStepType.END, toOptimize,"toList");
        toOptimize = new FunctionCallExpression(toOptimize,"size");
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), getExpectedGremlinForTestAddClosureWithExitExpressionDifferentFromExpr());

    }

    protected abstract String getExpectedGremlinForTestAddClosureWithExitExpressionDifferentFromExpr();

    @Test
    public void testAddClosureNoExitExpression() throws AtlasException {

        GroovyExpression expr1 = makeHasExpression("prop1","Fred");
        GroovyExpression expr2 = makeHasExpression("prop2","George");
        GroovyExpression toOptimize = getFactory().generateLogicalExpression(getVerticesExpression(), "or", Arrays.asList(expr1, expr2));
        toOptimize = makeOutExpression(toOptimize, "knows");
        toOptimize = makeOutExpression(toOptimize, "livesIn");
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), getExpectedGremlinForTestAddClosureNoExitExpression());
    }

    protected abstract String getExpectedGremlinForTestAddClosureNoExitExpression();


    private GroovyExpression makeOutExpression(GroovyExpression parent, String label) {
        return getFactory().generateAdjacentVerticesExpression(parent, AtlasEdgeDirection.OUT, label);
    }

    @Test
    public void testAddClosureWithExitExpressionEqualToExpr() throws AtlasException {

        GroovyExpression expr1 = makeHasExpression("prop1","Fred");
        GroovyExpression expr2 = makeHasExpression("prop2","George");
        GroovyExpression toOptimize = getFactory().generateLogicalExpression(getVerticesExpression(), "or", Arrays.asList(expr1, expr2));

        toOptimize = makeOutExpression(toOptimize, "knows");
        toOptimize = makeOutExpression(toOptimize, "livesIn");
        toOptimize = new FunctionCallExpression(TraversalStepType.END, toOptimize,"toList");
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), getExpectedGremlinForTestAddClosureWithExitExpressionEqualToExpr());
    }

    protected abstract String getExpectedGremlinForTestAddClosureWithExitExpressionEqualToExpr();


    @Test
    public void testClosureNotCreatedWhenNoOrs() throws AtlasException {

        GroovyExpression expr1 = makeHasExpression("prop1","Fred");
        GroovyExpression expr2 = makeHasExpression("prop2","George");
        GroovyExpression toOptimize = getFactory().generateLogicalExpression(getVerticesExpression(), "and", Arrays.asList(expr1, expr2));
        toOptimize = makeOutExpression(toOptimize, "knows");
        toOptimize = makeOutExpression(toOptimize, "livesIn");
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), getExpectedGremlinForTestClosureNotCreatedWhenNoOrs());
    }

    protected abstract String getExpectedGremlinForTestClosureNotCreatedWhenNoOrs();


    private GroovyExpression makeHasExpression(String name, String value) throws AtlasException {
        return makeHasExpression(null, name, value);
    }
    private GroovyExpression makeHasExpression(GroovyExpression parent, String name, String value) throws AtlasException {
        return getFactory().generateHasExpression(STRATEGY, parent, name, "=", new LiteralExpression(value), getTestFieldInfo());
    }
    private GroovyExpression makeFieldExpression(GroovyExpression parent, String fieldName) throws AtlasException {
        return getFactory().generateFieldExpression(parent, getTestFieldInfo(), fieldName, false);
    }

    @Test
    public void testOrFollowedByAnd() throws AtlasException {
        GroovyExpression expr1 = makeHasExpression("name","Fred");
        GroovyExpression expr2 = makeHasExpression("name","George");
        GroovyExpression expr3 = makeHasExpression("age","13");
        GroovyExpression expr4 = makeHasExpression("age","14");

        GroovyExpression toOptimize = getFactory().generateLogicalExpression(getVerticesExpression(), "or", Arrays.asList(expr1,expr2));
        toOptimize = getFactory().generateLogicalExpression(toOptimize, "and", Arrays.asList(expr3, expr4));

        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), getExpectedGremlinForTestOrFollowedByAnd());
    }

    protected abstract String getExpectedGremlinForTestOrFollowedByAnd();

    @Test
    public void testOrFollowedByOr() throws AtlasException {
        GroovyExpression expr1 = makeHasExpression("name","Fred");
        GroovyExpression expr2 = makeHasExpression("name","George");
        GroovyExpression expr3 = makeHasExpression("age","13");
        GroovyExpression expr4 = makeHasExpression("age","14");

        GroovyExpression toOptimize = getFactory().generateLogicalExpression(getVerticesExpression(), "or", Arrays.asList(expr1,expr2));
        toOptimize = getFactory().generateLogicalExpression(toOptimize, "or", Arrays.asList(expr3, expr4));

        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), getExpectedGremlinForTestOrFollowedByOr());
    }

    protected abstract String getExpectedGremlinForTestOrFollowedByOr();

    @Test
    public void testMassiveOrExpansion() throws AtlasException {
        GroovyExpression toOptimize = getVerticesExpression();
        toOptimize = makeHasExpression(toOptimize, "h1","h2");
        toOptimize = makeHasExpression(toOptimize, "h3","h4");
        for(int i = 0; i < 5; i++) {
            GroovyExpression expr1 = makeHasExpression("p1" + i,"e1" + i);
            GroovyExpression expr2 = makeHasExpression("p2" + i,"e2" + i);
            toOptimize = getFactory().generateLogicalExpression(toOptimize, "or", Arrays.asList(expr1,expr2));
            toOptimize = makeHasExpression(toOptimize, "ha" + i,"hb" + i);
            toOptimize = makeHasExpression(toOptimize, "hc" + i,"hd" + i);
        }
        toOptimize = makeHasExpression(toOptimize, "h5","h6");
        toOptimize = makeHasExpression(toOptimize, "h7","h8");

        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), getExpectedGremlinForTestMassiveOrExpansion());
    }

    protected abstract String getExpectedGremlinForTestMassiveOrExpansion();

    @Test
    public void testAndFollowedByAnd() throws AtlasException {
        GroovyExpression expr1 = makeHasExpression("name","Fred");
        GroovyExpression expr2 = makeHasExpression("name","George");
        GroovyExpression expr3 = makeHasExpression("age","13");
        GroovyExpression expr4 = makeHasExpression("age","14");

        GroovyExpression toOptimize = getFactory().generateLogicalExpression(getVerticesExpression(), "and", Arrays.asList(expr1,expr2));
        toOptimize = getFactory().generateLogicalExpression(toOptimize, "and", Arrays.asList(expr3, expr4));

        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), getExpectedGremlinForTestAndFollowedByAnd());


    }

    protected abstract String getExpectedGremlinForTestAndFollowedByAnd();

    @Test
    public void testAndFollowedByOr() throws AtlasException {
        GroovyExpression expr1 = makeHasExpression("name","Fred");
        GroovyExpression expr2 = makeHasExpression("name","George");
        GroovyExpression expr3 = makeHasExpression("age","13");
        GroovyExpression expr4 = makeHasExpression("age","14");

        GroovyExpression toOptimize = getFactory().generateLogicalExpression(getVerticesExpression(), "and", Arrays.asList(expr1,expr2));
        toOptimize = getFactory().generateLogicalExpression(toOptimize, "or", Arrays.asList(expr3, expr4));

        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), getExpectedGremlinForTestAndFollowedByOr());
    }

    protected abstract String getExpectedGremlinForTestAndFollowedByOr();

    @Test
    public void testInitialAlias() throws AtlasException {
        GroovyExpression expr1 = makeHasExpression("name","Fred");
        GroovyExpression expr2 = makeHasExpression("name","George");


        GroovyExpression toOptimize = getVerticesExpression();
        toOptimize = getFactory().generateAliasExpression(toOptimize, "x");
        toOptimize = getFactory().generateLogicalExpression(toOptimize, "or", Arrays.asList(expr1, expr2));
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), getExpectedGremlinForTestInitialAlias());
    }

    protected abstract String getExpectedGremlinForTestInitialAlias();

    @Test
    public void testFinalAlias() throws AtlasException {
        GroovyExpression expr1 = makeHasExpression("name","Fred");
        GroovyExpression expr2 = makeHasExpression("name","George");

        GroovyExpression toOptimize = getVerticesExpression();
        toOptimize = getFactory().generateLogicalExpression(toOptimize, "or", Arrays.asList(expr1, expr2));
        toOptimize = getFactory().generateAliasExpression(toOptimize, "x");
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), getExpectedGremlinForTestFinalAlias());
    }

    protected abstract String getExpectedGremlinForTestFinalAlias();

    @Test
    public void testAliasInMiddle() throws AtlasException {
        GroovyExpression expr1 = makeHasExpression("name","Fred");
        GroovyExpression expr2 = makeHasExpression("name","George");
        GroovyExpression expr3 = makeHasExpression("age","13");
        GroovyExpression expr4 = makeHasExpression("age","14");


        GroovyExpression toOptimize = getVerticesExpression();
        toOptimize = getFactory().generateLogicalExpression(toOptimize, "or", Arrays.asList(expr1, expr2));
        toOptimize = getFactory().generateAliasExpression(toOptimize, "x");
        toOptimize = getFactory().generateLogicalExpression(toOptimize, "or", Arrays.asList(expr3, expr4));
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), getExpectedGremlinForTestAliasInMiddle());
    }

    protected abstract String getExpectedGremlinForTestAliasInMiddle();

    @Test
    public void testMultipleAliases() throws AtlasException {
        GroovyExpression expr1 = makeHasExpression("name","Fred");
        GroovyExpression expr2 = makeHasExpression("name","George");
        GroovyExpression expr3 = makeHasExpression("age","13");
        GroovyExpression expr4 = makeHasExpression("age","14");


        GroovyExpression toOptimize = getVerticesExpression();
        toOptimize = getFactory().generateLogicalExpression(toOptimize, "or", Arrays.asList(expr1, expr2));
        toOptimize = getFactory().generateAliasExpression(toOptimize, "x");
        toOptimize = getFactory().generateLogicalExpression(toOptimize, "or", Arrays.asList(expr3, expr4));
        toOptimize = getFactory().generateAliasExpression(toOptimize, "y");
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), getExpectedGreminForTestMultipleAliases());
    }

    protected abstract String getExpectedGreminForTestMultipleAliases();

    @Test
    public void testAliasInOrExpr() throws AtlasException {
        GroovyExpression expr1 = makeHasExpression("name","Fred");
        GroovyExpression expr2 = getFactory().generateAliasExpression(makeHasExpression("name","George"), "george");

        GroovyExpression toOptimize = getVerticesExpression();
        toOptimize = getFactory().generateLogicalExpression(toOptimize, "or", Arrays.asList(expr1, expr2));
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), getExpectedGremlinForTestAliasInOrExpr());
    }

    protected abstract String getExpectedGremlinForTestAliasInOrExpr();

    @Test
    public void testAliasInAndExpr() throws AtlasException {
        GroovyExpression expr1 = makeHasExpression("name","Fred");
        GroovyExpression expr2 = getFactory().generateAliasExpression(makeHasExpression("name","George"), "george");

        GroovyExpression toOptimize = getVerticesExpression();
        toOptimize = getFactory().generateLogicalExpression(toOptimize, "and", Arrays.asList(expr1, expr2));
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        //expression with alias cannot currently be pulled out of the and
        assertEquals(optimized.toString(), getExpectedGremlinForTestAliasInAndExpr());
    }


    protected abstract String getExpectedGremlinForTestAliasInAndExpr();
    @Test
    public void testFlatMapExprInAnd() throws AtlasException {
        GroovyExpression expr1 = makeHasExpression("name","Fred");
        GroovyExpression expr2 = makeHasExpression(makeOutExpression(null,"knows"), "name","George");

        GroovyExpression toOptimize = getVerticesExpression();
        toOptimize = getFactory().generateLogicalExpression(toOptimize, "and", Arrays.asList(expr1, expr2));
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), getExpectedGremlinForTestFlatMapExprInAnd());
    }


    protected abstract String getExpectedGremlinForTestFlatMapExprInAnd();
    @Test
    public void testFlatMapExprInOr() throws AtlasException {
        GroovyExpression expr1 = makeHasExpression("name","Fred");
        GroovyExpression expr2 = makeHasExpression(makeOutExpression(null,"knows"), "name","George");

        GroovyExpression toOptimize = getVerticesExpression();
        toOptimize = getFactory().generateLogicalExpression(toOptimize, "or", Arrays.asList(expr1, expr2));
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), getExpectedGremlinForTestFlatMapExprInOr());
    }

    protected abstract String getExpectedGremlinForTestFlatMapExprInOr();

    @Test
    public void testFieldExpressionPushedToResultExpression() throws AtlasException {
        GroovyExpression expr1 = makeHasExpression("name","Fred");
        GroovyExpression expr2 = makeHasExpression(makeOutExpression(null,"knows"), "name","George");

        GroovyExpression toOptimize = getVerticesExpression();
        toOptimize = getFactory().generateLogicalExpression(toOptimize, "or", Arrays.asList(expr1, expr2));
        toOptimize = makeFieldExpression(toOptimize, "name");
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), getExpectedGremlinForTestFieldExpressionPushedToResultExpression());
    }

    protected abstract String getExpectedGremlinForTestFieldExpressionPushedToResultExpression();

    @Test
    public void testOrWithNoChildren() throws AtlasException {
        GroovyExpression toOptimize = getVerticesExpression();
        GroovyExpression expr1 = makeHasExpression(toOptimize, "name","Fred");

        toOptimize = getFactory().generateLogicalExpression(expr1, "or", Collections.<GroovyExpression>emptyList());
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        //or with no children matches no vertices
        assertEquals(optimized.toString(), getExpectedGremlinFortestOrWithNoChildren());
    }

    protected abstract String getExpectedGremlinFortestOrWithNoChildren();

    @Test
    public void testFinalAliasNeeded() throws AtlasException {
        GroovyExpression toOptimize = getVerticesExpression();
        toOptimize = makeHasExpression(toOptimize, "name", "Fred");
        toOptimize = getFactory().generateAliasExpression(toOptimize, "person");
        toOptimize = makeOutExpression(toOptimize, "livesIn");
        GroovyExpression isChicago = makeHasExpression(null, "name", "Chicago");
        GroovyExpression isBoston = makeHasExpression(null, "name", "Boston");
        toOptimize = getFactory().generateLogicalExpression(toOptimize, "or", Arrays.asList(isChicago, isBoston));
        toOptimize = getFactory().generateAliasExpression(toOptimize, "city");
        toOptimize = makeOutExpression(toOptimize, "state");
        toOptimize = makeHasExpression(toOptimize, "name", "Massachusetts");
        toOptimize = getFactory().generatePathExpression(toOptimize);
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), getExpectedGremlinForTestFinalAliasNeeded());
    }

    protected abstract String getExpectedGremlinForTestFinalAliasNeeded();

    @Test
    public void testSimpleRangeExpression() throws AtlasException {
        GroovyExpression expr1 = makeHasExpression(null, "name","Fred");
        GroovyExpression expr2 = makeHasExpression(null, "name","George");
        GroovyExpression expr3 = makeHasExpression(null, "age","34");
        GroovyExpression expr4 = makeHasExpression(null, "size","small");

        GroovyExpression toOptimize = getVerticesExpression();
        toOptimize = getFactory().generateLogicalExpression(toOptimize, "or", Arrays.asList(expr1, expr2));
        toOptimize = getFactory().generateLogicalExpression(toOptimize, "and", Collections.singletonList(expr3));
        toOptimize = getFactory().generateAdjacentVerticesExpression(toOptimize, AtlasEdgeDirection.OUT, "eats");
        toOptimize = getFactory().generateLogicalExpression(toOptimize, "and", Collections.singletonList(expr4));
        toOptimize = makeHasExpression(toOptimize, "color","blue");
        toOptimize = getFactory().generateRangeExpression(toOptimize, 0, 10);
        toOptimize = new FunctionCallExpression(TraversalStepType.END, toOptimize, "toList");
        toOptimize = new FunctionCallExpression(toOptimize, "size");
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), getExpectedGremlinForTestSimpleRangeExpression());
    }

    protected abstract String getExpectedGremlinForTestSimpleRangeExpression();


    @Test
    public void testRangeWithNonZeroOffset() throws Exception {
        // g.V().or(has('__typeName','OMAS_OMRSAsset'),has('__superTypeNames','OMAS_OMRSAsset')).range(5,10).as('inst').select('inst')
        GroovyExpression toOptimize = getVerticesExpression();

        GroovyExpression expr0 = makeHasExpression("__typeName", "OMAS_OMRSAsset");
        GroovyExpression expr1 = makeHasExpression("__superTypeNames", "OMAS_OMRSAsset");
        toOptimize = getFactory().generateLogicalExpression(toOptimize, "or", Arrays.asList(expr0, expr1));
        toOptimize = getFactory().generateRangeExpression(toOptimize, 5, 10);
        toOptimize = getFactory().generateAliasExpression(toOptimize, "inst");
        toOptimize = getFactory().generateSelectExpression(toOptimize, Collections.singletonList(new LiteralExpression("inst")), Collections.<GroovyExpression>emptyList());
        RangeFinder visitor = new RangeFinder(getFactory());
        GremlinQueryOptimizer.visitCallHierarchy(toOptimize, visitor);
        List<AbstractFunctionExpression> rangeExpressions = visitor.getRangeExpressions();
        assertEquals(rangeExpressions.size(), 1);
        int[] rangeParameters = getFactory().getRangeParameters(rangeExpressions.get(0));
        assertNotNull(rangeParameters);
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        // The range optimization is not supported with a non-zero start index, so the optimizer should not add range expressions
        // to the expanded or's.
        assertEquals(optimized.toString(), getExpectedGremlinForTestRangeWithNonZeroOffset());
    }

    protected abstract String getExpectedGremlinForTestRangeWithNonZeroOffset();

    @Test
    public void testRangeWithOrderBy() throws Exception {
        // The range optimization is not supported with order, so the optimizer should not add range expressions
        // to the expanded or's.
        GroovyExpression toOptimize = getVerticesExpression();

        GroovyExpression expr0 = makeHasExpression("__typeName", "OMAS_OMRSAsset");
        GroovyExpression expr1 = makeHasExpression("__superTypeNames", "OMAS_OMRSAsset");
        toOptimize = getFactory().generateLogicalExpression(toOptimize, "or", Arrays.asList(expr0, expr1));
        toOptimize = getFactory().generateRangeExpression(toOptimize, 5, 10);
        toOptimize = getFactory().generateAliasExpression(toOptimize, "inst");
        //toOptimize = getFactory().generateSelectExpression(toOptimize, Collections.singletonList(new LiteralExpression("inst")), Collections.<GroovyExpression>emptyList());
        GroovyExpression orderFielda = makeFieldExpression(getFactory().getCurrentTraverserObject(getFactory().getClosureArgumentValue()), "name");
        GroovyExpression orderFieldb = makeFieldExpression(getFactory().getCurrentTraverserObject(getFactory().getClosureArgumentValue()), "name");
        toOptimize = getFactory().generateOrderByExpression(toOptimize,Arrays.asList(orderFielda, orderFieldb), true);
        RangeFinder visitor = new RangeFinder(getFactory());
        GremlinQueryOptimizer.visitCallHierarchy(toOptimize, visitor);
        List<AbstractFunctionExpression> rangeExpressions = visitor.getRangeExpressions();
        assertEquals(rangeExpressions.size(), 1);
        int[] rangeParameters = getFactory().getRangeParameters(rangeExpressions.get(0));
        assertNotNull(rangeParameters);
        GroovyExpression optimized = GremlinQueryOptimizer.getInstance().optimize(toOptimize);
        assertEquals(optimized.toString(), getExpectedGremlinForTestRangeWithOrderBy());
    }



    protected abstract String getExpectedGremlinForTestRangeWithOrderBy();

    @Override
    public AtlasGraph get() throws RepositoryException {
        AtlasGraph graph = mock(AtlasGraph.class);
        when(graph.getSupportedGremlinVersion()).thenReturn(GremlinVersion.THREE);
        when(graph.isPropertyValueConversionNeeded(any(IDataType.class))).thenReturn(false);
        return graph;
    }
}
