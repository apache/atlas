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

import org.apache.atlas.gremlin.Gremlin3ExpressionFactory;
import org.apache.atlas.gremlin.GremlinExpressionFactory;
import org.testng.annotations.Test;


@Test
public class Gremlin3QueryOptimizerTest extends AbstractGremlinQueryOptimizerTest {

    public static GremlinExpressionFactory FACTORY = null;

    @Override
    protected GremlinExpressionFactory getFactory() {
        if (null == FACTORY) {
            FACTORY = new Gremlin3ExpressionFactory();
        }
        return FACTORY;
    }

    @Override
    protected String getExpectedGremlinForTestPullHasExpressionsOutOfHas() {
        return "g.V().has('prop1',eq('Fred')).has('prop2',eq('George')).and(out('out1'),out('out2'))";
    }

    @Override
    protected String getExpectedGremlinForTestOrGrouping() {
        return "def r=(([]) as Set);"
                + "g.V().has('prop1',eq('Fred')).fill(r);"
                + "g.V().has('prop2',eq('George')).fill(r);"
                + "g.V().or(out('out1'),out('out2')).fill(r);"
                + "r";
    }

    @Override
    protected String getExpectedGremlinForTestAndOfOrs() {

        return "def r=(([]) as Set);"
                + "g.V().has('p1',eq('e1')).has('p3',eq('e3')).fill(r);"
                + "g.V().has('p1',eq('e1')).has('p4',eq('e4')).fill(r);"
                + "g.V().has('p2',eq('e2')).has('p3',eq('e3')).fill(r);"
                + "g.V().has('p2',eq('e2')).has('p4',eq('e4')).fill(r);"
                + "r";
    }

    @Override
    protected String getExpectedGremlinForTestAndWithMultiCallArguments() {

        return "g.V().has('p1',eq('e1')).has('p2',eq('e2')).has('p3',eq('e3')).has('p4',eq('e4'))";
    }

    @Override
    protected String getExpectedGremlinForTestOrOfAnds() {
        return  "def r=(([]) as Set);"
                + "g.V().has('p1',eq('e1')).has('p2',eq('e2')).fill(r);"
                + "g.V().has('p3',eq('e3')).has('p4',eq('e4')).fill(r);"
                + "r";
    }

    @Override
    protected String getExpectedGremlinForTestHasNotMovedToResult() {
        return "def r=(([]) as Set);"
                + "def f1={GraphTraversal x->x.has('p3',eq('e3')).as('_src').select('_src').fill(r)};"
                + "f1(g.V().has('p1',eq('e1')));f1(g.V().has('p2',eq('e2')));"
                + "g.V('').inject(((r) as Vertex[])).as('_src').select('src1').by((({it}) as Function))";
    }


    @Override
    protected String getExpectedGremlinForTestLongStringEndingWithOr() {
        return "def r=(([]) as Set);"
                + "def f1={g.V().has('name',eq('Fred')).has('age',eq('13')).out('livesIn').has('state',eq('Massachusetts'))};"
                + "def f2={GraphTraversal x->x.has('p5',eq('e5')).has('p6',eq('e6'))};"
                + "f2(f1().has('p1',eq('e1')).has('p3',eq('e3'))).has('p7',eq('e7')).fill(r);"
                + "f2(f1().has('p1',eq('e1')).has('p3',eq('e3'))).has('p8',eq('e8')).fill(r);"
                + "f2(f1().has('p1',eq('e1')).has('p4',eq('e4'))).has('p7',eq('e7')).fill(r);"
                + "f2(f1().has('p1',eq('e1')).has('p4',eq('e4'))).has('p8',eq('e8')).fill(r);"
                + "f2(f1().has('p2',eq('e2')).has('p3',eq('e3'))).has('p7',eq('e7')).fill(r);"
                + "f2(f1().has('p2',eq('e2')).has('p3',eq('e3'))).has('p8',eq('e8')).fill(r);"
                + "f2(f1().has('p2',eq('e2')).has('p4',eq('e4'))).has('p7',eq('e7')).fill(r);"
                + "f2(f1().has('p2',eq('e2')).has('p4',eq('e4'))).has('p8',eq('e8')).fill(r);"
                + "r";
    }

    @Override
    protected String getExpectedGremlinForTestLongStringNotEndingWithOr() {
        return "def r=(([]) as Set);"
                + "def f1={g.V().has('name',eq('Fred')).has('age',eq('13')).out('livesIn').has('state',eq('Massachusetts'))};"
                + "def f2={GraphTraversal x->x.has('p5',eq('e5')).has('p6',eq('e6'))};"
                + "def f3={GraphTraversal x->x.has('p9',eq('e9')).fill(r)};"
                + "f3(f2(f1().has('p1',eq('e1')).has('p3',eq('e3'))).has('p7',eq('e7')));"
                + "f3(f2(f1().has('p1',eq('e1')).has('p3',eq('e3'))).has('p8',eq('e8')));"
                + "f3(f2(f1().has('p1',eq('e1')).has('p4',eq('e4'))).has('p7',eq('e7')));"
                + "f3(f2(f1().has('p1',eq('e1')).has('p4',eq('e4'))).has('p8',eq('e8')));"
                + "f3(f2(f1().has('p2',eq('e2')).has('p3',eq('e3'))).has('p7',eq('e7')));"
                + "f3(f2(f1().has('p2',eq('e2')).has('p3',eq('e3'))).has('p8',eq('e8')));"
                + "f3(f2(f1().has('p2',eq('e2')).has('p4',eq('e4'))).has('p7',eq('e7')));"
                + "f3(f2(f1().has('p2',eq('e2')).has('p4',eq('e4'))).has('p8',eq('e8')));"
                + "r";
    }

    @Override
    protected String getExpectedGremlinForTestToListConversion() {

        return "def r=(([]) as Set);"
                + "g.V().has('prop1',eq('Fred')).fill(r);"
                + "g.V().has('prop2',eq('George')).fill(r);"
                + "g.V('').inject(((r) as Vertex[])).toList()";
    }


    @Override
    protected String getExpectedGremlinForTestToListWithExtraStuff() {
        return "def r=(([]) as Set);"
                + "g.V().has('prop1',eq('Fred')).fill(r);"
                + "g.V().has('prop2',eq('George')).fill(r);"
                + "g.V('').inject(((r) as Vertex[])).toList().size()";
    }


    @Override
    protected String getExpectedGremlinForTestAddClosureWithExitExpressionDifferentFromExpr() {
        return "def r=(([]) as Set);"
                + "g.V().has('prop1',eq('Fred')).out('knows').out('livesIn').fill(r);"
                + "g.V().has('prop2',eq('George')).out('knows').out('livesIn').fill(r);"
                + "g.V('').inject(((r) as Vertex[])).toList().size()";
    }

    @Override
    protected String getExpectedGremlinForTestAddClosureNoExitExpression() {
        return "def r=(([]) as Set);"
                + "g.V().has('prop1',eq('Fred')).out('knows').out('livesIn').fill(r);"
                + "g.V().has('prop2',eq('George')).out('knows').out('livesIn').fill(r);"
                + "r";
    }

    @Override
    protected String getExpectedGremlinForTestAddClosureWithExitExpressionEqualToExpr() {
        return "def r=(([]) as Set);"
                + "g.V().has('prop1',eq('Fred')).out('knows').out('livesIn').fill(r);"
                + "g.V().has('prop2',eq('George')).out('knows').out('livesIn').fill(r);"
                + "g.V('').inject(((r) as Vertex[])).toList()";
    }

    @Override
    protected String getExpectedGremlinForTestClosureNotCreatedWhenNoOrs() {
        return "g.V().has('prop1',eq('Fred')).has('prop2',eq('George')).out('knows').out('livesIn')";
    }

    @Override
    protected String getExpectedGremlinForTestOrFollowedByAnd() {
        return "def r=(([]) as Set);"
                + "def f1={GraphTraversal x->x.has('age',eq('13')).has('age',eq('14')).fill(r)};"
                + "f1(g.V().has('name',eq('Fred')));"
                + "f1(g.V().has('name',eq('George')));"
                + "r";
    }


    @Override
    protected String getExpectedGremlinForTestOrFollowedByOr() {
        return "def r=(([]) as Set);"
                + "g.V().has('name',eq('Fred')).has('age',eq('13')).fill(r);"
                + "g.V().has('name',eq('Fred')).has('age',eq('14')).fill(r);"
                + "g.V().has('name',eq('George')).has('age',eq('13')).fill(r);"
                + "g.V().has('name',eq('George')).has('age',eq('14')).fill(r);"
                + "r";
    }

    @Override
    protected String getExpectedGremlinForTestMassiveOrExpansion() {
        return "def r=(([]) as Set);"
                + "def f1={g.V().has('h1',eq('h2')).has('h3',eq('h4'))};"
                + "def f2={GraphTraversal x->x.has('ha0',eq('hb0')).has('hc0',eq('hd0'))};"
                + "def f3={GraphTraversal x->x.has('ha1',eq('hb1')).has('hc1',eq('hd1'))};"
                + "def f4={GraphTraversal x->x.has('ha2',eq('hb2')).has('hc2',eq('hd2'))};"
                + "def f5={GraphTraversal x->x.has('ha3',eq('hb3')).has('hc3',eq('hd3'))};"
                + "def f6={GraphTraversal x->x.has('ha4',eq('hb4')).has('hc4',eq('hd4')).has('h5',eq('h6')).has('h7',eq('h8')).fill(r)};"
                + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p11',eq('e11'))).has('p12',eq('e12'))).has('p13',eq('e13'))).has('p14',eq('e14')));"
                + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p11',eq('e11'))).has('p12',eq('e12'))).has('p13',eq('e13'))).has('p24',eq('e24')));"
                + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p11',eq('e11'))).has('p12',eq('e12'))).has('p23',eq('e23'))).has('p14',eq('e14')));"
                + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p11',eq('e11'))).has('p12',eq('e12'))).has('p23',eq('e23'))).has('p24',eq('e24')));"
                + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p11',eq('e11'))).has('p22',eq('e22'))).has('p13',eq('e13'))).has('p14',eq('e14')));"
                + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p11',eq('e11'))).has('p22',eq('e22'))).has('p13',eq('e13'))).has('p24',eq('e24')));"
                + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p11',eq('e11'))).has('p22',eq('e22'))).has('p23',eq('e23'))).has('p14',eq('e14')));"
                + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p11',eq('e11'))).has('p22',eq('e22'))).has('p23',eq('e23'))).has('p24',eq('e24')));"
                + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p21',eq('e21'))).has('p12',eq('e12'))).has('p13',eq('e13'))).has('p14',eq('e14')));"
                + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p21',eq('e21'))).has('p12',eq('e12'))).has('p13',eq('e13'))).has('p24',eq('e24')));"
                + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p21',eq('e21'))).has('p12',eq('e12'))).has('p23',eq('e23'))).has('p14',eq('e14')));"
                + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p21',eq('e21'))).has('p12',eq('e12'))).has('p23',eq('e23'))).has('p24',eq('e24')));"
                + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p21',eq('e21'))).has('p22',eq('e22'))).has('p13',eq('e13'))).has('p14',eq('e14')));"
                + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p21',eq('e21'))).has('p22',eq('e22'))).has('p13',eq('e13'))).has('p24',eq('e24')));"
                + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p21',eq('e21'))).has('p22',eq('e22'))).has('p23',eq('e23'))).has('p14',eq('e14')));"
                + "f6(f5(f4(f3(f2(f1().has('p10',eq('e10'))).has('p21',eq('e21'))).has('p22',eq('e22'))).has('p23',eq('e23'))).has('p24',eq('e24')));"
                + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p11',eq('e11'))).has('p12',eq('e12'))).has('p13',eq('e13'))).has('p14',eq('e14')));"
                + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p11',eq('e11'))).has('p12',eq('e12'))).has('p13',eq('e13'))).has('p24',eq('e24')));"
                + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p11',eq('e11'))).has('p12',eq('e12'))).has('p23',eq('e23'))).has('p14',eq('e14')));"
                + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p11',eq('e11'))).has('p12',eq('e12'))).has('p23',eq('e23'))).has('p24',eq('e24')));"
                + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p11',eq('e11'))).has('p22',eq('e22'))).has('p13',eq('e13'))).has('p14',eq('e14')));"
                + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p11',eq('e11'))).has('p22',eq('e22'))).has('p13',eq('e13'))).has('p24',eq('e24')));"
                + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p11',eq('e11'))).has('p22',eq('e22'))).has('p23',eq('e23'))).has('p14',eq('e14')));"
                + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p11',eq('e11'))).has('p22',eq('e22'))).has('p23',eq('e23'))).has('p24',eq('e24')));"
                + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p21',eq('e21'))).has('p12',eq('e12'))).has('p13',eq('e13'))).has('p14',eq('e14')));"
                + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p21',eq('e21'))).has('p12',eq('e12'))).has('p13',eq('e13'))).has('p24',eq('e24')));"
                + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p21',eq('e21'))).has('p12',eq('e12'))).has('p23',eq('e23'))).has('p14',eq('e14')));"
                + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p21',eq('e21'))).has('p12',eq('e12'))).has('p23',eq('e23'))).has('p24',eq('e24')));"
                + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p21',eq('e21'))).has('p22',eq('e22'))).has('p13',eq('e13'))).has('p14',eq('e14')));"
                + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p21',eq('e21'))).has('p22',eq('e22'))).has('p13',eq('e13'))).has('p24',eq('e24')));"
                + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p21',eq('e21'))).has('p22',eq('e22'))).has('p23',eq('e23'))).has('p14',eq('e14')));"
                + "f6(f5(f4(f3(f2(f1().has('p20',eq('e20'))).has('p21',eq('e21'))).has('p22',eq('e22'))).has('p23',eq('e23'))).has('p24',eq('e24')));"
                + "r";

    }

    @Override
    protected String getExpectedGremlinForTestAndFollowedByAnd() {
        return "g.V().has('name',eq('Fred')).has('name',eq('George')).has('age',eq('13')).has('age',eq('14'))";
    }


    @Override
    protected String getExpectedGremlinForTestAndFollowedByOr() {
        return "def r=(([]) as Set);"
                + "def f1={g.V().has('name',eq('Fred')).has('name',eq('George'))};"
                + "f1().has('age',eq('13')).fill(r);"
                + "f1().has('age',eq('14')).fill(r);"
                + "r";
    }

    @Override
    protected String getExpectedGremlinForTestInitialAlias() {
        return "def r=(([]) as Set);"
                + "g.V().as('x').has('name',eq('Fred')).fill(r);"
                + "g.V().as('x').has('name',eq('George')).fill(r);"
                + "r";
    }

    @Override
    protected String getExpectedGremlinForTestFinalAlias() {
        return "def r=(([]) as Set);"
                + "g.V().has('name',eq('Fred')).as('x').fill(r);"
                + "g.V().has('name',eq('George')).as('x').fill(r);"
                + "r";
    }

    @Override
    protected String getExpectedGremlinForTestAliasInMiddle() {
        return "def r=(([]) as Set);"
                + "g.V().has('name',eq('Fred')).as('x').has('age',eq('13')).fill(r);"
                + "g.V().has('name',eq('Fred')).as('x').has('age',eq('14')).fill(r);"
                + "g.V().has('name',eq('George')).as('x').has('age',eq('13')).fill(r);"
                + "g.V().has('name',eq('George')).as('x').has('age',eq('14')).fill(r);"
                + "r";
    }

    @Override
    protected String getExpectedGreminForTestMultipleAliases() {
        return "def r=(([]) as Set);"
                + "def f1={GraphTraversal x->x.as('y').fill(r)};"
                + "f1(g.V().has('name',eq('Fred')).as('x').has('age',eq('13')));"
                + "f1(g.V().has('name',eq('Fred')).as('x').has('age',eq('14')));"
                + "f1(g.V().has('name',eq('George')).as('x').has('age',eq('13')));"
                + "f1(g.V().has('name',eq('George')).as('x').has('age',eq('14')));"
                + "r";
    }

    @Override
    protected String getExpectedGremlinForTestAliasInOrExpr() {
        return "def r=(([]) as Set);"
                + "g.V().has('name',eq('Fred')).fill(r);"
                + "g.V().or(has('name',eq('George')).as('george')).fill(r);"
                + "r";
    }

    @Override
    protected String getExpectedGremlinForTestAliasInAndExpr() {
        return "g.V().has('name',eq('Fred')).and(has('name',eq('George')).as('george'))";
    }

    @Override
    protected String getExpectedGremlinForTestFlatMapExprInAnd() {
        return "g.V().has('name',eq('Fred')).and(out('knows').has('name',eq('George')))";
    }

    @Override
    protected String getExpectedGremlinForTestFlatMapExprInOr() {
        return "def r=(([]) as Set);"
                + "g.V().has('name',eq('Fred')).fill(r);"
                + "g.V().or(out('knows').has('name',eq('George'))).fill(r);"
                + "r";
    }

    @Override
    protected String getExpectedGremlinForTestFieldExpressionPushedToResultExpression() {

        return "def r=(([]) as Set);"
                + "g.V().has('name',eq('Fred')).fill(r);"
                + "g.V().or(out('knows').has('name',eq('George'))).fill(r);"
                + "g.V('').inject(((r) as Vertex[])).values('name')";
    }

    @Override
    protected String getExpectedGremlinFortestOrWithNoChildren() {
        return "def r=(([]) as Set);"
                + "r";
    }

    @Override
    protected String getExpectedGremlinForTestFinalAliasNeeded() {
        return "def r=(([]) as Set);"
                + "def f1={g.V().has('name',eq('Fred')).as('person').out('livesIn')};"
                + "def f2={GraphTraversal x->x.as('city').out('state').has('name',eq('Massachusetts')).as('__res').select('person','city','__res').fill(r)};"
                + "f2(f1().has('name',eq('Chicago')));f2(f1().has('name',eq('Boston')));"
                + "__(((r) as Map[])).as('__tmp').map({((Map)it.get()).get('person')}).as('person').select('__tmp').map({((Map)it.get()).get('city')}).as('city').select('__tmp').map({((Map)it.get()).get('__res')}).as('__res').path().toList().collect({it.tail()})";
    }

    @Override
    protected String getExpectedGremlinForTestSimpleRangeExpression() {
        return "def r=(([]) as Set);"
                + "def f1={GraphTraversal x->x.has('age',eq('34')).out('eats').has('size',eq('small')).has('color',eq('blue')).range(0,10).fill(r)};"
                + "f1(g.V().has('name',eq('Fred')));"
                + "f1(g.V().has('name',eq('George')));"
                + "g.V('').inject(((r) as Vertex[])).range(0,10).toList().size()";
    }

    @Override
    protected String getExpectedGremlinForOptimizeLoopExpression() {
        return "def r=(([]) as Set);def f1={GraphTraversal x->x.has('name',eq('Fred')).as('label').select('label').fill(r)};"
                + "f1(g.V().has('__typeName','DataSet'));"
                + "f1(g.V().has('__superTypeNames','DataSet'));"
                + "g.V('').inject(((r) as Vertex[])).as('label').repeat(__.in('inputTables').out('outputTables')).emit(has('__typeName',eq('string')).or().has('__superTypeNames',eq('string'))).toList()";
    }

    @Override
    protected String getExpectedGremlinForTestRangeWithNonZeroOffset() {
        return "def r=(([]) as Set);" +
               "g.V().has('__typeName',eq('OMAS_OMRSAsset')).fill(r);" +
               "g.V().has('__superTypeNames',eq('OMAS_OMRSAsset')).fill(r);" +
               "g.V('').inject(((r) as Vertex[])).range(5,10).as('inst').select('inst')";
    }

    @Override
    protected String getExpectedGremlinForTestRangeWithOrderBy() {
        return "def r=(([]) as Set);"
                + "g.V().has('__typeName',eq('OMAS_OMRSAsset')).fill(r);"
                + "g.V().has('__superTypeNames',eq('OMAS_OMRSAsset')).fill(r);"
                + "g.V('').inject(((r) as Vertex[])).range(5,10).as('inst').order().by((({it.get().values('name')}) as Function),{a, b->a.toString().toLowerCase() <=> b.toString().toLowerCase()})";
    }

}
