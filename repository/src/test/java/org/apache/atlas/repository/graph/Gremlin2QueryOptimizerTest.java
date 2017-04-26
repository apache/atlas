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

import org.apache.atlas.gremlin.Gremlin2ExpressionFactory;
import org.apache.atlas.gremlin.GremlinExpressionFactory;
import org.testng.annotations.Test;


@Test
public class Gremlin2QueryOptimizerTest extends AbstractGremlinQueryOptimizerTest {
    private static GremlinExpressionFactory FACTORY = null;

    @Override
    protected GremlinExpressionFactory getFactory() {
        if (null == FACTORY) {
            FACTORY = new Gremlin2ExpressionFactory();
        }
        return FACTORY;
    }

    @Override
    protected String getExpectedGremlinForTestPullHasExpressionsOutOfHas() {
        return "g.V().has('prop1',T.'eq','Fred').has('prop2',T.'eq','George').and(out('out1'),out('out2'))";
    }

    @Override
    protected String getExpectedGremlinForTestOrGrouping() {
        return "def r=(([]) as Set);"
                + "g.V().has('prop1',T.'eq','Fred').fill(r);"
                + "g.V().has('prop2',T.'eq','George').fill(r);"
                + "g.V().or(out('out1'),out('out2')).fill(r);"
                + "r";
    }

    @Override
    protected String getExpectedGremlinForTestAndOfOrs() {
        return  "def r=(([]) as Set);"
                + "g.V().has('p1',T.'eq','e1').has('p3',T.'eq','e3').fill(r);"
                + "g.V().has('p1',T.'eq','e1').has('p4',T.'eq','e4').fill(r);"
                + "g.V().has('p2',T.'eq','e2').has('p3',T.'eq','e3').fill(r);"
                + "g.V().has('p2',T.'eq','e2').has('p4',T.'eq','e4').fill(r);"
                + "r";
    }


    @Override
    protected String getExpectedGremlinForTestAndWithMultiCallArguments() {
        return "g.V().has('p1',T.'eq','e1').has('p2',T.'eq','e2').has('p3',T.'eq','e3').has('p4',T.'eq','e4')";
    }

    @Override
    protected String getExpectedGremlinForTestOrOfAnds() {

        return "def r=(([]) as Set);"
                + "g.V().has('p1',T.'eq','e1').has('p2',T.'eq','e2').fill(r);"
                + "g.V().has('p3',T.'eq','e3').has('p4',T.'eq','e4').fill(r);"
                + "r";
    }

    @Override
    protected String getExpectedGremlinForTestHasNotMovedToResult() {
        return "def r=(([]) as Set);"
                + "def f1={GremlinPipeline x->x.has('p3',T.'eq','e3').as('_src').select(['_src']).fill(r)};"
                + "f1(g.V().has('p1',T.'eq','e1'));"
                + "f1(g.V().has('p2',T.'eq','e2'));"
                + "r._().transform({((Row)it).getColumn('_src')}).as('_src').select(['src1'],{it})";
    }

    @Override
    protected String getExpectedGremlinForOptimizeLoopExpression() {
        return "def r=(([]) as Set);"
                + "g.V().has('__typeName','DataSet').has('name',T.'eq','Fred').fill(r);"
                + "g.V().has('__superTypeNames','DataSet').has('name',T.'eq','Fred').fill(r);"
                + "r._().as('label').in('inputTables').out('outputTables').loop('label',{((it.'path'.contains(it.'object'))?(false):(true))},{it.'object'.'__typeName' == 'string' || ((it.'object'.'__superTypeNames')?(it.'object'.'__superTypeNames'.contains('string')):(false))}).enablePath().toList()";
    }


    @Override
    protected String getExpectedGremlinForTestLongStringEndingWithOr() {
        return "def r=(([]) as Set);"
                + "def f1={g.V().has('name',T.'eq','Fred').has('age',T.'eq','13').out('livesIn').has('state',T.'eq','Massachusetts')};"
                + "def f2={GremlinPipeline x->x.has('p5',T.'eq','e5').has('p6',T.'eq','e6')};"
                + "f2(f1().has('p1',T.'eq','e1').has('p3',T.'eq','e3')).has('p7',T.'eq','e7').fill(r);"
                + "f2(f1().has('p1',T.'eq','e1').has('p3',T.'eq','e3')).has('p8',T.'eq','e8').fill(r);"
                + "f2(f1().has('p1',T.'eq','e1').has('p4',T.'eq','e4')).has('p7',T.'eq','e7').fill(r);"
                + "f2(f1().has('p1',T.'eq','e1').has('p4',T.'eq','e4')).has('p8',T.'eq','e8').fill(r);"
                + "f2(f1().has('p2',T.'eq','e2').has('p3',T.'eq','e3')).has('p7',T.'eq','e7').fill(r);"
                + "f2(f1().has('p2',T.'eq','e2').has('p3',T.'eq','e3')).has('p8',T.'eq','e8').fill(r);"
                + "f2(f1().has('p2',T.'eq','e2').has('p4',T.'eq','e4')).has('p7',T.'eq','e7').fill(r);"
                + "f2(f1().has('p2',T.'eq','e2').has('p4',T.'eq','e4')).has('p8',T.'eq','e8').fill(r);"
                + "r";
    }

    @Override
    protected String getExpectedGremlinForTestLongStringNotEndingWithOr() {
        return "def r=(([]) as Set);"
                + "def f1={g.V().has('name',T.'eq','Fred').has('age',T.'eq','13').out('livesIn').has('state',T.'eq','Massachusetts')};"
                + "def f2={GremlinPipeline x->x.has('p5',T.'eq','e5').has('p6',T.'eq','e6')};"
                + "def f3={GremlinPipeline x->x.has('p9',T.'eq','e9').fill(r)};"
                + "f3(f2(f1().has('p1',T.'eq','e1').has('p3',T.'eq','e3')).has('p7',T.'eq','e7'));"
                + "f3(f2(f1().has('p1',T.'eq','e1').has('p3',T.'eq','e3')).has('p8',T.'eq','e8'));"
                + "f3(f2(f1().has('p1',T.'eq','e1').has('p4',T.'eq','e4')).has('p7',T.'eq','e7'));"
                + "f3(f2(f1().has('p1',T.'eq','e1').has('p4',T.'eq','e4')).has('p8',T.'eq','e8'));"
                + "f3(f2(f1().has('p2',T.'eq','e2').has('p3',T.'eq','e3')).has('p7',T.'eq','e7'));"
                + "f3(f2(f1().has('p2',T.'eq','e2').has('p3',T.'eq','e3')).has('p8',T.'eq','e8'));"
                + "f3(f2(f1().has('p2',T.'eq','e2').has('p4',T.'eq','e4')).has('p7',T.'eq','e7'));"
                + "f3(f2(f1().has('p2',T.'eq','e2').has('p4',T.'eq','e4')).has('p8',T.'eq','e8'));"
                + "r";
    }

    @Override
    protected String getExpectedGremlinForTestToListConversion() {
        return "def r=(([]) as Set);"
                + "g.V().has('prop1',T.'eq','Fred').fill(r);"
                + "g.V().has('prop2',T.'eq','George').fill(r);"
                + "r._().toList()";
    }

    @Override
    protected String getExpectedGremlinForTestToListWithExtraStuff() {
        return "def r=(([]) as Set);"
                + "g.V().has('prop1',T.'eq','Fred').fill(r);"
                + "g.V().has('prop2',T.'eq','George').fill(r);"
                + "r._().toList().size()";
    }


    @Override
    protected String getExpectedGremlinForTestAddClosureWithExitExpressionDifferentFromExpr() {
        return "def r=(([]) as Set);"
                + "g.V().has('prop1',T.'eq','Fred').out('knows').out('livesIn').fill(r);"
                + "g.V().has('prop2',T.'eq','George').out('knows').out('livesIn').fill(r);"
                + "r._().toList().size()";
    }

    @Override
    protected String getExpectedGremlinForTestAddClosureNoExitExpression() {
        return "def r=(([]) as Set);"
                + "g.V().has('prop1',T.'eq','Fred').out('knows').out('livesIn').fill(r);"
                + "g.V().has('prop2',T.'eq','George').out('knows').out('livesIn').fill(r);"
                + "r";
    }

    @Override
    protected String getExpectedGremlinForTestAddClosureWithExitExpressionEqualToExpr() {
        return "def r=(([]) as Set);"
                + "g.V().has('prop1',T.'eq','Fred').out('knows').out('livesIn').fill(r);"
                + "g.V().has('prop2',T.'eq','George').out('knows').out('livesIn').fill(r);"
                + "r._().toList()";
    }

    @Override
    protected String getExpectedGremlinForTestClosureNotCreatedWhenNoOrs() {
        return "g.V().has('prop1',T.'eq','Fred').has('prop2',T.'eq','George').out('knows').out('livesIn')";
    }

    @Override
    protected String getExpectedGremlinForTestOrFollowedByAnd() {
        return "def r=(([]) as Set);"
                + "def f1={GremlinPipeline x->x.has('age',T.'eq','13').has('age',T.'eq','14').fill(r)};"
                + "f1(g.V().has('name',T.'eq','Fred'));"
                + "f1(g.V().has('name',T.'eq','George'));"
                + "r";
    }

    @Override
    protected String getExpectedGremlinForTestOrFollowedByOr() {
        return "def r=(([]) as Set);"
                + "g.V().has('name',T.'eq','Fred').has('age',T.'eq','13').fill(r);"
                + "g.V().has('name',T.'eq','Fred').has('age',T.'eq','14').fill(r);"
                + "g.V().has('name',T.'eq','George').has('age',T.'eq','13').fill(r);"
                + "g.V().has('name',T.'eq','George').has('age',T.'eq','14').fill(r);"
                + "r";
    }

    @Override
    protected String getExpectedGremlinForTestMassiveOrExpansion() {
        return "def r=(([]) as Set);"
                + "def f1={g.V().has('h1',T.'eq','h2').has('h3',T.'eq','h4')};"
                + "def f2={GremlinPipeline x->x.has('ha0',T.'eq','hb0').has('hc0',T.'eq','hd0')};"
                + "def f3={GremlinPipeline x->x.has('ha1',T.'eq','hb1').has('hc1',T.'eq','hd1')};"
                + "def f4={GremlinPipeline x->x.has('ha2',T.'eq','hb2').has('hc2',T.'eq','hd2')};"
                + "def f5={GremlinPipeline x->x.has('ha3',T.'eq','hb3').has('hc3',T.'eq','hd3')};"
                + "def f6={GremlinPipeline x->x.has('ha4',T.'eq','hb4').has('hc4',T.'eq','hd4').has('h5',T.'eq','h6').has('h7',T.'eq','h8').fill(r)};"
                + "f6(f5(f4(f3(f2(f1().has('p10',T.'eq','e10')).has('p11',T.'eq','e11')).has('p12',T.'eq','e12')).has('p13',T.'eq','e13')).has('p14',T.'eq','e14'));"
                + "f6(f5(f4(f3(f2(f1().has('p10',T.'eq','e10')).has('p11',T.'eq','e11')).has('p12',T.'eq','e12')).has('p13',T.'eq','e13')).has('p24',T.'eq','e24'));"
                + "f6(f5(f4(f3(f2(f1().has('p10',T.'eq','e10')).has('p11',T.'eq','e11')).has('p12',T.'eq','e12')).has('p23',T.'eq','e23')).has('p14',T.'eq','e14'));"
                + "f6(f5(f4(f3(f2(f1().has('p10',T.'eq','e10')).has('p11',T.'eq','e11')).has('p12',T.'eq','e12')).has('p23',T.'eq','e23')).has('p24',T.'eq','e24'));"
                + "f6(f5(f4(f3(f2(f1().has('p10',T.'eq','e10')).has('p11',T.'eq','e11')).has('p22',T.'eq','e22')).has('p13',T.'eq','e13')).has('p14',T.'eq','e14'));"
                + "f6(f5(f4(f3(f2(f1().has('p10',T.'eq','e10')).has('p11',T.'eq','e11')).has('p22',T.'eq','e22')).has('p13',T.'eq','e13')).has('p24',T.'eq','e24'));"
                + "f6(f5(f4(f3(f2(f1().has('p10',T.'eq','e10')).has('p11',T.'eq','e11')).has('p22',T.'eq','e22')).has('p23',T.'eq','e23')).has('p14',T.'eq','e14'));"
                + "f6(f5(f4(f3(f2(f1().has('p10',T.'eq','e10')).has('p11',T.'eq','e11')).has('p22',T.'eq','e22')).has('p23',T.'eq','e23')).has('p24',T.'eq','e24'));"
                + "f6(f5(f4(f3(f2(f1().has('p10',T.'eq','e10')).has('p21',T.'eq','e21')).has('p12',T.'eq','e12')).has('p13',T.'eq','e13')).has('p14',T.'eq','e14'));"
                + "f6(f5(f4(f3(f2(f1().has('p10',T.'eq','e10')).has('p21',T.'eq','e21')).has('p12',T.'eq','e12')).has('p13',T.'eq','e13')).has('p24',T.'eq','e24'));"
                + "f6(f5(f4(f3(f2(f1().has('p10',T.'eq','e10')).has('p21',T.'eq','e21')).has('p12',T.'eq','e12')).has('p23',T.'eq','e23')).has('p14',T.'eq','e14'));"
                + "f6(f5(f4(f3(f2(f1().has('p10',T.'eq','e10')).has('p21',T.'eq','e21')).has('p12',T.'eq','e12')).has('p23',T.'eq','e23')).has('p24',T.'eq','e24'));"
                + "f6(f5(f4(f3(f2(f1().has('p10',T.'eq','e10')).has('p21',T.'eq','e21')).has('p22',T.'eq','e22')).has('p13',T.'eq','e13')).has('p14',T.'eq','e14'));"
                + "f6(f5(f4(f3(f2(f1().has('p10',T.'eq','e10')).has('p21',T.'eq','e21')).has('p22',T.'eq','e22')).has('p13',T.'eq','e13')).has('p24',T.'eq','e24'));"
                + "f6(f5(f4(f3(f2(f1().has('p10',T.'eq','e10')).has('p21',T.'eq','e21')).has('p22',T.'eq','e22')).has('p23',T.'eq','e23')).has('p14',T.'eq','e14'));"
                + "f6(f5(f4(f3(f2(f1().has('p10',T.'eq','e10')).has('p21',T.'eq','e21')).has('p22',T.'eq','e22')).has('p23',T.'eq','e23')).has('p24',T.'eq','e24'));"
                + "f6(f5(f4(f3(f2(f1().has('p20',T.'eq','e20')).has('p11',T.'eq','e11')).has('p12',T.'eq','e12')).has('p13',T.'eq','e13')).has('p14',T.'eq','e14'));"
                + "f6(f5(f4(f3(f2(f1().has('p20',T.'eq','e20')).has('p11',T.'eq','e11')).has('p12',T.'eq','e12')).has('p13',T.'eq','e13')).has('p24',T.'eq','e24'));"
                + "f6(f5(f4(f3(f2(f1().has('p20',T.'eq','e20')).has('p11',T.'eq','e11')).has('p12',T.'eq','e12')).has('p23',T.'eq','e23')).has('p14',T.'eq','e14'));"
                + "f6(f5(f4(f3(f2(f1().has('p20',T.'eq','e20')).has('p11',T.'eq','e11')).has('p12',T.'eq','e12')).has('p23',T.'eq','e23')).has('p24',T.'eq','e24'));"
                + "f6(f5(f4(f3(f2(f1().has('p20',T.'eq','e20')).has('p11',T.'eq','e11')).has('p22',T.'eq','e22')).has('p13',T.'eq','e13')).has('p14',T.'eq','e14'));"
                + "f6(f5(f4(f3(f2(f1().has('p20',T.'eq','e20')).has('p11',T.'eq','e11')).has('p22',T.'eq','e22')).has('p13',T.'eq','e13')).has('p24',T.'eq','e24'));"
                + "f6(f5(f4(f3(f2(f1().has('p20',T.'eq','e20')).has('p11',T.'eq','e11')).has('p22',T.'eq','e22')).has('p23',T.'eq','e23')).has('p14',T.'eq','e14'));"
                + "f6(f5(f4(f3(f2(f1().has('p20',T.'eq','e20')).has('p11',T.'eq','e11')).has('p22',T.'eq','e22')).has('p23',T.'eq','e23')).has('p24',T.'eq','e24'));"
                + "f6(f5(f4(f3(f2(f1().has('p20',T.'eq','e20')).has('p21',T.'eq','e21')).has('p12',T.'eq','e12')).has('p13',T.'eq','e13')).has('p14',T.'eq','e14'));"
                + "f6(f5(f4(f3(f2(f1().has('p20',T.'eq','e20')).has('p21',T.'eq','e21')).has('p12',T.'eq','e12')).has('p13',T.'eq','e13')).has('p24',T.'eq','e24'));"
                + "f6(f5(f4(f3(f2(f1().has('p20',T.'eq','e20')).has('p21',T.'eq','e21')).has('p12',T.'eq','e12')).has('p23',T.'eq','e23')).has('p14',T.'eq','e14'));"
                + "f6(f5(f4(f3(f2(f1().has('p20',T.'eq','e20')).has('p21',T.'eq','e21')).has('p12',T.'eq','e12')).has('p23',T.'eq','e23')).has('p24',T.'eq','e24'));"
                + "f6(f5(f4(f3(f2(f1().has('p20',T.'eq','e20')).has('p21',T.'eq','e21')).has('p22',T.'eq','e22')).has('p13',T.'eq','e13')).has('p14',T.'eq','e14'));"
                + "f6(f5(f4(f3(f2(f1().has('p20',T.'eq','e20')).has('p21',T.'eq','e21')).has('p22',T.'eq','e22')).has('p13',T.'eq','e13')).has('p24',T.'eq','e24'));"
                + "f6(f5(f4(f3(f2(f1().has('p20',T.'eq','e20')).has('p21',T.'eq','e21')).has('p22',T.'eq','e22')).has('p23',T.'eq','e23')).has('p14',T.'eq','e14'));"
                + "f6(f5(f4(f3(f2(f1().has('p20',T.'eq','e20')).has('p21',T.'eq','e21')).has('p22',T.'eq','e22')).has('p23',T.'eq','e23')).has('p24',T.'eq','e24'));"
                + "r";

    }

    @Override
    protected String getExpectedGremlinForTestAndFollowedByAnd() {
        return "g.V().has('name',T.'eq','Fred').has('name',T.'eq','George').has('age',T.'eq','13').has('age',T.'eq','14')";

    }

    @Override
    protected String getExpectedGremlinForTestAndFollowedByOr() {
        return "def r=(([]) as Set);"
                + "def f1={g.V().has('name',T.'eq','Fred').has('name',T.'eq','George')};f1().has('age',T.'eq','13').fill(r);"
                + "f1().has('age',T.'eq','14').fill(r);"
                + "r";
    }

    @Override
    protected String getExpectedGremlinForTestInitialAlias() {
        return "def r=(([]) as Set);"
                + "g.V().as('x').has('name',T.'eq','Fred').fill(r);"
                + "g.V().as('x').has('name',T.'eq','George').fill(r);"
                + "r";
    }

    @Override
    protected String getExpectedGremlinForTestFinalAlias() {
        return "def r=(([]) as Set);"
                + "g.V().has('name',T.'eq','Fred').as('x').fill(r);"
                + "g.V().has('name',T.'eq','George').as('x').fill(r);"
                + "r";
    }

    @Override
    protected String getExpectedGremlinForTestAliasInMiddle() {
        return "def r=(([]) as Set);"
                + "g.V().has('name',T.'eq','Fred').as('x').has('age',T.'eq','13').fill(r);"
                + "g.V().has('name',T.'eq','Fred').as('x').has('age',T.'eq','14').fill(r);"
                + "g.V().has('name',T.'eq','George').as('x').has('age',T.'eq','13').fill(r);"
                + "g.V().has('name',T.'eq','George').as('x').has('age',T.'eq','14').fill(r);"
                + "r";
    }

    @Override
    protected String getExpectedGreminForTestMultipleAliases() {
        return "def r=(([]) as Set);"
                + "def f1={GremlinPipeline x->x.as('y').fill(r)};"
                + "f1(g.V().has('name',T.'eq','Fred').as('x').has('age',T.'eq','13'));"
                + "f1(g.V().has('name',T.'eq','Fred').as('x').has('age',T.'eq','14'));"
                + "f1(g.V().has('name',T.'eq','George').as('x').has('age',T.'eq','13'));"
                + "f1(g.V().has('name',T.'eq','George').as('x').has('age',T.'eq','14'));"
                + "r";
    }


    @Override
    protected String getExpectedGremlinForTestAliasInOrExpr() {
        return "def r=(([]) as Set);"
                + "g.V().has('name',T.'eq','Fred').fill(r);"
                + "g.V().or(has('name',T.'eq','George').as('george')).fill(r);"
                + "r";
    }

    @Override
    protected String getExpectedGremlinForTestAliasInAndExpr() {
        return "g.V().has('name',T.'eq','Fred').and(has('name',T.'eq','George').as('george'))";
    }
    @Override
    protected String getExpectedGremlinForTestFlatMapExprInAnd() {
        return "g.V().has('name',T.'eq','Fred').and(out('knows').has('name',T.'eq','George'))";
    }

    @Override
    protected String getExpectedGremlinForTestFlatMapExprInOr() {
        return "def r=(([]) as Set);"
                + "g.V().has('name',T.'eq','Fred').fill(r);"
                + "g.V().or(out('knows').has('name',T.'eq','George')).fill(r);"
                + "r";
    }


    @Override
    protected String getExpectedGremlinForTestFieldExpressionPushedToResultExpression() {
        return "def r=(([]) as Set);"
                + "g.V().has('name',T.'eq','Fred').fill(r);"
                + "g.V().or(out('knows').has('name',T.'eq','George')).fill(r);"
                + "r._().'name'";
    }

    @Override
    protected String getExpectedGremlinFortestOrWithNoChildren() {
        return "def r=(([]) as Set);"
                + "r";
    }

    @Override
    protected String getExpectedGremlinForTestFinalAliasNeeded() {
        return "def r=(([]) as Set);"
                + "def f1={g.V().has('name',T.'eq','Fred').as('person').out('livesIn')};"
                + "def f2={GremlinPipeline x->x.as('city').out('state').has('name',T.'eq','Massachusetts').as('__res').select(['person', 'city', '__res']).fill(r)};"
                + "f2(f1().has('name',T.'eq','Chicago'));"
                + "f2(f1().has('name',T.'eq','Boston'));"
                + "r._().as('__tmp').transform({((Row)it).getColumn('person')}).as('person').back('__tmp').transform({((Row)it).getColumn('city')}).as('city').back('__tmp').transform({((Row)it).getColumn('__res')}).as('__res').path().toList().collect({it.tail()})";
    }

    @Override
    protected String getExpectedGremlinForTestSimpleRangeExpression() {
        return "def r=(([]) as Set);"
                + "def f1={GremlinPipeline x->x.has('age',T.'eq','34').out('eats').has('size',T.'eq','small').has('color',T.'eq','blue') [0..<10].fill(r)};"
                + "f1(g.V().has('name',T.'eq','Fred'));"
                + "f1(g.V().has('name',T.'eq','George'));"
                + "r._() [0..<10].toList().size()";
    }

    @Override
    protected String getExpectedGremlinForTestRangeWithNonZeroOffset() {
        return "def r=(([]) as Set);"
                + "g.V().has('__typeName',T.'eq','OMAS_OMRSAsset').fill(r);"
                + "g.V().has('__superTypeNames',T.'eq','OMAS_OMRSAsset').fill(r);"
                + "r._() [5..<10].as('inst').select(['inst'])";
    }

    @Override
    protected String getExpectedGremlinForTestRangeWithOrderBy() {
        return "def r=(([]) as Set);"
                + "g.V().has('__typeName',T.'eq','OMAS_OMRSAsset').fill(r);"
                + "g.V().has('__superTypeNames',T.'eq','OMAS_OMRSAsset').fill(r);"
                + "r._() [5..<10].as('inst').order({((it.'name' != null)?(it.'name'.toLowerCase()):(it.'name')) <=> ((it.'name' != null)?(it.'name'.toLowerCase()):(it.'name'))})";
    }

}
