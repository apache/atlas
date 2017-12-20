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

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenStream;
import org.apache.atlas.query.antlr4.AtlasDSLLexer;
import org.apache.atlas.query.antlr4.AtlasDSLParser;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.lang.StringUtils;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class QueryProcessorTest {
    private List<String> errorList = new ArrayList<>();

    @Test
    public void classification() {
        String expected = "g.V().has('__traitNames', within('PII')).limit(25).toList()";
        verify("PII", expected);
    }

    @Test()
    public void dimension() {
        String expected = "g.V().has('__typeName', 'Table').has('__traitNames', within('Dimension')).limit(25).toList()";
        verify("Table isa Dimension", expected);
        verify("Table is Dimension", expected);
        verify("Table where Table is Dimension", expected);
        verify("Table isa Dimension where name = 'sales'",
                "g.V().has('__typeName', 'Table').has('__traitNames', within('Dimension')).has('Table.name', eq('sales')).limit(25).toList()");
    }

    @Test
    public void fromDB() {
        verify("from DB", "g.V().has('__typeName', 'DB').limit(25).toList()");
        verify("from DB limit 10", "g.V().has('__typeName', 'DB').limit(10).toList()");
        verify("DB limit 10", "g.V().has('__typeName', 'DB').limit(10).toList()");
    }

    @Test
    public void DBHasName() {
        String expected = "g.V().has('__typeName', 'DB').has('DB.name').limit(25).toList()";
        verify("DB has name", expected);
        verify("DB where DB has name", expected);
    }

    @Test
    public void DBasD() {
        verify("DB as d", "g.V().has('__typeName', 'DB').as('d').limit(25).toList()");
    }

    @Test
    public void DBasDSelect() {
        String expected = "def f(r){ return [['d.name','d.owner']].plus(r.collect({[it.value('DB.name'),it.value('DB.owner')]})).unique(); }; f(g.V().has('__typeName', 'DB').as('d')";
        verify("DB as d select d.name, d.owner", expected + ".limit(25).toList())");
        verify("DB as d select d.name, d.owner limit 10", expected + ".limit(10).toList())");
    }

    @Test
    public void tableSelectColumns() {
        String exMain = "g.V().has('__typeName', 'Table').out('__Table.columns').limit(10).toList()";
        String exSel = "def f(r){ r }";
        verify("Table select columns limit 10", getExpected(exSel, exMain));

        String exMain2 = "g.V().has('__typeName', 'Table').out('__Table.db').limit(25).toList()";
        verify("Table select db.name", getExpected(exSel, exMain2));
    }

    @Test(enabled = false)
    public void DBTableFrom() {
        verify("Table, db", "g.V().has('__typeName', 'Table').out('__DB.Table').limit(25).toList()");
    }

    @Test
    public void DBAsDSelectLimit() {
        verify("from DB limit 5", "g.V().has('__typeName', 'DB').limit(5).toList()");
        verify("from DB limit 5 offset 2", "g.V().has('__typeName', 'DB').range(2, 2 + 5).limit(25).toList()");
    }

    @Test
    public void DBOrderBy() {
        String expected = "g.V().has('__typeName', 'DB').order().by('DB.name').limit(25).toList()";
        verify("DB orderby name", expected);
        verify("from DB orderby name", expected);
        verify("from DB as d orderby d.owner limit 3", "g.V().has('__typeName', 'DB').as('d').order().by('DB.owner').limit(3).toList()");
        verify("DB as d orderby d.owner limit 3", "g.V().has('__typeName', 'DB').as('d').order().by('DB.owner').limit(3).toList()");


        String exSel = "def f(r){ return [['d.name','d.owner']].plus(r.collect({[it.value('DB.name'),it.value('DB.owner')]})).unique(); }";
        String exMain = "g.V().has('__typeName', 'DB').as('d').order().by('DB.owner)').limit(25).toList()";
        verify("DB as d select d.name, d.owner orderby (d.owner) limit 25", getExpected(exSel, exMain));

        String exMain2 = "g.V().has('__typeName', 'Table').and(__.has('Table.name', eq(\"sales_fact\")),__.has('Table.createTime', gt('1388563200000'))).order().by('Table.createTime').limit(25).toList()";
        String exSel2 = "def f(r){ return [['_col_0','_col_1']].plus(r.collect({[it.value('Table.name'),it.value('Table.createTime')]})).unique(); }";
        verify("Table where (name = \"sales_fact\" and createTime > \"2014-01-01\" ) select name as _col_0, createTime as _col_1 orderby _col_1",
                getExpected(exSel2, exMain2));
    }

    @Test
    public void fromDBOrderByNameDesc() {
        verify("from DB orderby name DESC", "g.V().has('__typeName', 'DB').order().by('DB.name', decr).limit(25).toList()");
    }

    @Test
    public void fromDBSelect() {
        String expected = "def f(r){ return [['DB.name','DB.owner']].plus(r.collect({[it.value('DB.name'),it.value('DB.owner')]})).unique(); }; f(g.V().has('__typeName', 'DB').limit(25).toList())";
        verify("from DB select DB.name, DB.owner", expected);
    }

    @Test
    public void fromDBGroupBy() {
        verify("from DB groupby (DB.owner)", "g.V().has('__typeName', 'DB').group().by('DB.owner').limit(25).toList()");
    }

    @Test
    public void whereClauseTextContains() {
        String exMain = "g.V().has('__typeName', 'DB').has('DB.name', eq(\"Reporting\")).limit(25).toList()";
        String exSel = "def f(r){ return [['name','owner']].plus(r.collect({[it.value('DB.name'),it.value('DB.owner')]})).unique(); }";
        verify("from DB where name = \"Reporting\" select name, owner", getExpected(exSel, exMain));
        verify("from DB where (name = \"Reporting\") select name, owner", getExpected(exSel, exMain));
        verify("Table where Asset.name like \"Tab*\"",
                "g.V().has('__typeName', 'Table').has('Table.name', org.janusgraph.core.attribute.Text.textRegex(\"Tab.*\")).limit(25).toList()");
        verify("from Table where (db.name = \"Reporting\")",
                "g.V().has('__typeName', 'Table').out('__Table.db').has('DB.name', eq(\"Reporting\")).in('__Table.db').limit(25).toList()");
    }

    @Test
    public void whereClauseWithAsTextContains() {
        String exSel = "def f(r){ return [['t.name','t.owner']].plus(r.collect({[it.value('Table.name'),it.value('Table.owner')]})).unique(); }";
        String exMain = "g.V().has('__typeName', 'Table').as('t').has('Table.name', eq(\"testtable_1\")).limit(25).toList()";
        verify("Table as t where t.name = \"testtable_1\" select t.name, t.owner)", getExpected(exSel, exMain));
    }

    @Test
    public void whereClauseWithDateCompare() {
        String exSel = "def f(r){ return [['t.name','t.owner']].plus(r.collect({[it.value('Table.name'),it.value('Table.owner')]})).unique(); }";
        String exMain = "g.V().has('__typeName', 'Table').as('t').has('Table.createdTime', eq('1513046158440')).limit(25).toList()";
        verify("Table as t where t.createdTime = \"2017-12-12T02:35:58.440Z\" select t.name, t.owner)", getExpected(exSel, exMain));
    }

    @Test
    public void multipleWhereClauses() {
        String exSel = "def f(r){ return [['c.owner','c.name','c.dataType']].plus(r.collect({[it.value('Column.owner'),it.value('Column.name'),it.value('Column.dataType')]})).unique(); }";
        String exMain = "g.V().has('__typeName', 'Table').has('Table.name', eq(\"sales_fact\")).out('__Table.columns').as('c').limit(25).toList()";
        verify("Table where name=\"sales_fact\", columns as c select c.owner, c.name, c.dataType", getExpected(exSel, exMain));
                ;
    }

    @Test
    public void subType() {
        String exMain = "g.V().has('__typeName', within('Asset','Table')).limit(25).toList()";
        String exSel = "def f(r){ return [['name','owner']].plus(r.collect({[it.value('Asset.name'),it.value('Asset.owner')]})).unique(); }";

        verify("Asset select name, owner", getExpected(exSel, exMain));
    }

    @Test
    public void TraitWithSpace() {
        verify("`Log Data`", "g.V().has('__typeName', 'Log Data').limit(25).toList()");
    }

    @Test
    public void nestedQueries() {
        verify("Table where name=\"sales_fact\" or name=\"testtable_1\"",
                "g.V().has('__typeName', 'Table').or(__.has('Table.name', eq(\"sales_fact\")),__.has('Table.name', eq(\"testtable_1\"))).limit(25).toList()");
        verify("Table where name=\"sales_fact\" and name=\"testtable_1\"",
                "g.V().has('__typeName', 'Table').and(__.has('Table.name', eq(\"sales_fact\")),__.has('Table.name', eq(\"testtable_1\"))).limit(25).toList()");
        verify("Table where name=\"sales_fact\" or name=\"testtable_1\" or name=\"testtable_2\"",
                "g.V().has('__typeName', 'Table')" +
                        ".or(" +
                        "__.has('Table.name', eq(\"sales_fact\"))," +
                        "__.has('Table.name', eq(\"testtable_1\"))," +
                        "__.has('Table.name', eq(\"testtable_2\"))" +
                        ").limit(25).toList()");
        verify("Table where name=\"sales_fact\" and name=\"testtable_1\" and name=\"testtable_2\"",
                "g.V().has('__typeName', 'Table')" +
                        ".and(" +
                        "__.has('Table.name', eq(\"sales_fact\"))," +
                        "__.has('Table.name', eq(\"testtable_1\"))," +
                        "__.has('Table.name', eq(\"testtable_2\"))" +
                        ").limit(25).toList()");
        verify("Table where (name=\"sales_fact\" or name=\"testtable_1\") and name=\"testtable_2\"",
                "g.V().has('__typeName', 'Table')" +
                        ".and(" +
                        "__.or(" +
                        "__.has('Table.name', eq(\"sales_fact\"))," +
                        "__.has('Table.name', eq(\"testtable_1\"))" +
                        ")," +
                        "__.has('Table.name', eq(\"testtable_2\")))" +
                        ".limit(25).toList()");
        verify("Table where name=\"sales_fact\" or (name=\"testtable_1\" and name=\"testtable_2\")",
                "g.V().has('__typeName', 'Table')" +
                        ".or(" +
                        "__.has('Table.name', eq(\"sales_fact\"))," +
                        "__.and(" +
                        "__.has('Table.name', eq(\"testtable_1\"))," +
                        "__.has('Table.name', eq(\"testtable_2\")))" +
                        ")" +
                        ".limit(25).toList()");
        verify("Table where name=\"sales_fact\" or name=\"testtable_1\" and name=\"testtable_2\"",
                "g.V().has('__typeName', 'Table')" +
                        ".and(" +
                        "__.or(" +
                        "__.has('Table.name', eq(\"sales_fact\"))," +
                        "__.has('Table.name', eq(\"testtable_1\"))" +
                        ")," +
                        "__.has('Table.name', eq(\"testtable_2\")))" +
                        ".limit(25).toList()");
        verify("Table where (name=\"sales_fact\" and owner=\"Joe\") OR (name=\"sales_fact_daily_mv\" and owner=\"Joe BI\")",
                "g.V().has('__typeName', 'Table')" +
                        ".or(" +
                        "__.and(" +
                        "__.has('Table.name', eq(\"sales_fact\"))," +
                        "__.has('Table.owner', eq(\"Joe\"))" +
                        ")," +
                        "__.and(" +
                        "__.has('Table.name', eq(\"sales_fact_daily_mv\"))," +
                        "__.has('Table.owner', eq(\"Joe BI\"))" +
                        "))" +
                        ".limit(25).toList()");
        verify("Table where owner=\"hdfs\" or ((name=\"testtable_1\" or name=\"testtable_2\") and createdTime < \"2017-12-12T02:35:58.440Z\")",
                "g.V().has('__typeName', 'Table').or(__.has('Table.owner', eq(\"hdfs\")),__.and(__.or(__.has('Table.name', eq(\"testtable_1\")),__.has('Table.name', eq(\"testtable_2\"))),__.has('Table.createdTime', lt('1513046158440')))).limit(25).toList()");
        verify("hive_db where hive_db.name='Reporting' and hive_db.createTime < '2017-12-12T02:35:58.440Z'",
                "g.V().has('__typeName', 'hive_db').and(__.has('hive_db.name', eq('Reporting')),__.has('hive_db.createTime', lt('1513046158440'))).limit(25).toList()");
        verify("Table where db.name='Sales' and db.clusterName='cl1'",
                "g.V().has('__typeName', 'Table').and(__.out('__Table.db').has('DB.name', eq('Sales')).in('__Table.db'),__.out('__Table.db').has('DB.clusterName', eq('cl1')).in('__Table.db')).limit(25).toList()");
    }

    private void verify(String dsl, String expectedGremlin) {
        AtlasDSLParser.QueryContext queryContext = getParsedQuery(dsl);
        String actualGremlin = getGremlinQuery(queryContext);
        assertEquals(actualGremlin, expectedGremlin);
    }

    private String getExpected(String select, String main) {
        return String.format("%s; f(%s)", select, main);
    }

    private AtlasDSLParser.QueryContext getParsedQuery(String query) {
        AtlasDSLParser.QueryContext queryContext = null;
        InputStream stream = new ByteArrayInputStream(query.getBytes());
        AtlasDSLLexer lexer = null;

        try {
            lexer = new AtlasDSLLexer(CharStreams.fromStream(stream));
        } catch (IOException e) {
            assertTrue(false);
        }

        TokenStream inputTokenStream = new CommonTokenStream(lexer);
        AtlasDSLParser parser = new AtlasDSLParser(inputTokenStream);
        queryContext = parser.query();

        assertNotNull(queryContext);
        assertNull(queryContext.exception);

        return queryContext;
    }

    private String getGremlinQuery(AtlasDSLParser.QueryContext queryContext) {
        AtlasTypeRegistry registry = mock(AtlasTypeRegistry.class);
        org.apache.atlas.query.Lookup lookup = new TestLookup(errorList, registry);
        QueryProcessor.Context context = new QueryProcessor.Context(errorList, lookup);

        QueryProcessor queryProcessor = new QueryProcessor(lookup, context);
        DSLVisitor qv = new DSLVisitor(queryProcessor);
        qv.visit(queryContext);
        queryProcessor.close();

        String s = queryProcessor.getText();
        assertTrue(StringUtils.isNotEmpty(s));
        return s;
    }

    private static class TestLookup implements org.apache.atlas.query.Lookup {

        List<String> errorList;
        AtlasTypeRegistry registry;

        public TestLookup(List<String> errorList, AtlasTypeRegistry typeRegistry) {
            this.errorList = errorList;
            this.registry = typeRegistry;
        }

        @Override
        public AtlasType getType(String typeName) {
            AtlasType type = null;
            if(typeName.equals("PII") || typeName.equals("Dimension")) {
                type = mock(AtlasType.class);
            } else {
                type = mock(AtlasEntityType.class);
            }

            when(type.getTypeName()).thenReturn(typeName);
            return type;
        }

        @Override
        public String getQualifiedName(QueryProcessor.Context context, String name) {
            if(name.contains("."))
                return name;

            return String.format("%s.%s", context.getActiveTypeName(), name);
        }

        @Override
        public boolean isPrimitive(QueryProcessor.Context context, String attributeName) {
            return attributeName.equals("name") ||
                    attributeName.equals("owner") ||
                    attributeName.equals("createdTime") ||
                    attributeName.equals("createTime") ||
                    attributeName.equals("clusterName");
        }

        @Override
        public String getRelationshipEdgeLabel(QueryProcessor.Context context, String attributeName) {
            if (attributeName.equalsIgnoreCase("columns"))
                return "__Table.columns";
            if (attributeName.equalsIgnoreCase("db"))
                return "__Table.db";
            else
                return "__DB.Table";
        }

        @Override
        public boolean hasAttribute(QueryProcessor.Context context, String typeName) {
            return (context.getActiveTypeName().equals("Table") && typeName.equals("db")) ||
                    (context.getActiveTypeName().equals("Table") && typeName.equals("columns"));
        }

        @Override
        public boolean doesTypeHaveSubTypes(QueryProcessor.Context context) {
            return context.getActiveTypeName().equalsIgnoreCase("Asset");
        }

        @Override
        public String getTypeAndSubTypes(QueryProcessor.Context context) {
            String[] str = new String[]{"'Asset'", "'Table'"};
            return StringUtils.join(str, ",");
        }

        @Override
        public boolean isTraitType(QueryProcessor.Context context) {
            return context.getActiveTypeName().equals("PII") || context.getActiveTypeName().equals("Dimension");
        }

        @Override
        public String getTypeFromEdge(QueryProcessor.Context context, String item) {
            if(context.getActiveTypeName().equals("DB") && item.equals("Table")) {
                return "Table";
            } else if(context.getActiveTypeName().equals("Table") && item.equals("Column")) {
                return "Column";
            } else if(context.getActiveTypeName().equals("Table") && item.equals("db")) {
                return "DB";
            } else if(context.getActiveTypeName().equals("Table") && item.equals("columns")) {
                return "Column";
            }
            return context.getActiveTypeName();
        }

        @Override
        public boolean isDate(QueryProcessor.Context context, String attributeName) {
            return attributeName.equals("createdTime") ||
                    attributeName.equals("createTime");
        }
    }
}
