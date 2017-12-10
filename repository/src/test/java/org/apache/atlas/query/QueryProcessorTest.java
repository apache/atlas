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
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.lang.StringUtils;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class QueryProcessorTest {
    private List<String> errorList = new ArrayList<>();

    @Test
    public void trait() {
        String expected = "g.V().has('__traitNames', within('PII')).limit(25).toList()";
        verify("PII", expected);
    }

    @Test()
    public void dimension() {
        String expected = "g.V().has('__typeName', 'Table').has('__traitNames', within('Dimension')).limit(25).toList()";
        verify("Table isa Dimension", expected);
        verify("Table is Dimension", expected);
        verify("Table where Table is Dimension", expected);
    }

    @Test
    public void fromDB() {
        verify("from DB", "g.V().has('__typeName', 'DB').limit(25).toList()");
        verify("from DB limit 10", "g.V().has('__typeName', 'DB').order().limit(10).toList()");

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
    public void tableSelectColumns() {
        verify("Table select Columns limit 10", "g.V().has('__typeName', 'Table').out('__Table.columns').as('s0').select('s0').order().limit(10).toList()");
    }

    @Test
    public void DBasDSelect() {
        String expected = "g.V().has('__typeName', 'DB').as('d').valueMap('DB.name', 'DB.owner')";
        verify("DB as d select d.name, d.owner", expected + ".limit(25).toList()");
        verify("DB as d select d.name, d.owner limit 10", expected + ".order().limit(10).toList()");
    }

    @Test
    public void DBTableFrom() {
        verify("DB, Table", "g.V().has('__typeName', 'DB').out('__DB.Table').limit(25).toList()");
    }

    @Test
    public void DBAsDSelectLimit() {
        verify("from DB limit 5", "g.V().has('__typeName', 'DB').order().limit(5).toList()");
        verify("from DB limit 5 offset 2", "g.V().has('__typeName', 'DB').order().range(2, 2 + 5).limit(25).toList()");
    }

    @Test
    public void DBOrderBy() {
        String expected = "g.V().has('__typeName', 'DB').order().by('DB.name').limit(25).toList()";
//        verify("DB orderby name", expected);
        verify("from DB orderby name", expected);
    }

    @Test
    public void fromDBOrderByNameDesc() {
        verify("from DB orderby name DESC", "g.V().has('__typeName', 'DB').order().by('DB.name', decr).limit(25).toList()");
    }

    @Test
    public void fromDBSelect() {
        verify("from DB select DB.name, DB.owner", "g.V().has('__typeName', 'DB').valueMap('DB.name', 'DB.owner').limit(25).toList()");
    }

    @Test
    public void fromDBSelectGroupBy() {
        verify("from DB groupby (DB.owner)", "g.V().has('__typeName', 'DB').group().by('DB.owner').limit(25).toList()");
    }

    @Test
    public void whereClauseTextContains() {
        String expected = "g.V().has('__typeName', 'DB').has('DB.name', eq(\"Reporting\")).valueMap('DB.name', 'DB.owner').limit(25).toList()";
        verify("from DB where name = \"Reporting\" select name, owner)", expected);
        verify("Table where Asset.name like \"Tab*\"",
                "g.V().has('__typeName', 'Table').has('Asset.name', org.janusgraph.core.attribute.Text.textContainsRegex(\"Tab.*\")).limit(25).toList()");
        verify("from DB where (name = \"Reporting\") select name, owner", expected);
        verify("from DB as db1 Table where (db1.name = \"Reporting\") select name, owner",
                "g.V().has('__typeName', 'DB').as('db1').out('__DB.Table').has('DB.name', eq(\"Reporting\")).valueMap('Column.name', 'Column.owner').limit(25).toList()");
    }

    @Test
    public void whereClauseWithAsTextContains() {
        verify("Table as t where t.name = \"testtable_1\" select t.name, t.owner)",
                "g.V().has('__typeName', 'Table').as('t').has('Table.name', eq(\"testtable_1\")).valueMap('Table.name', 'Table.owner').limit(25).toList()");
    }

    @Test
    public void multipleWhereClauses() {
        verify("Table where name=\"sales_fact\", columns as c select c.owner, c.name, c.dataType",
                "g.V().has('__typeName', 'Table').has('Table.name', eq(\"sales_fact\")).out('__Table.columns').as('c').valueMap('Column.owner', 'Column.name', 'Column.dataType').limit(25).toList()");
    }

    @Test
    public void subType() {
        verify("Asset select name, owner",
                "g.V().has('__typeName', within('Asset','Table')).valueMap('Asset.name', 'Asset.owner').limit(25).toList()");
    }

    @Test
    public void TraitWithSpace() {
        verify("`Log Data`", "g.V().has('__typeName', 'Log Data').limit(25).toList()");
    }

    private void verify(String dsl, String expectedGremlin) {
        AtlasDSLParser.QueryContext queryContext = getParsedQuery(dsl);
        String actualGremlin = getGremlinQuery(queryContext);
        assertEquals(actualGremlin, expectedGremlin);
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
        QueryProcessor queryProcessor = new QueryProcessor(new TestTypeRegistryLookup(errorList, mock(AtlasTypeRegistry.class)));
        DSLVisitor qv = new DSLVisitor(queryProcessor);
        qv.visit(queryContext);
        queryProcessor.close();

        assertTrue(StringUtils.isNotEmpty(queryProcessor.getText()));
        return queryProcessor.getText();
    }

    private static class TestTypeRegistryLookup extends QueryProcessor.TypeRegistryLookup {
        private String activeType;
        private HashMap<String, String> asContext = new HashMap<>();

        public TestTypeRegistryLookup(List<String> errorList, AtlasTypeRegistry typeRegistry) {
            super(errorList, typeRegistry);
        }

        public void registerActive(String typeName) {
            activeType = typeName;
        }

        public boolean hasActiveType() {
            return !StringUtils.isEmpty(activeType);
        }

        public void registerStepType(String stepName) {
            if (!asContext.containsKey(stepName)) {
                asContext.put(stepName, activeType);
            } else {
                addError(String.format("Multiple steps with same name detected: %s", stepName));
            }
        }

        public String getRelationshipEdgeLabelForActiveType(String item) {
            if(item.equalsIgnoreCase("columns"))
                return "__Table.columns";
            else
                return "__DB.Table";
        }

        public String getQualifiedAttributeName(String item) {
            if (item.contains(".")) {
                String[] keyValue = StringUtils.split(item, ".");

                if (!asContext.containsKey(keyValue[0])) {
                    return item;
                } else {
                    String s = getStitchedString(keyValue, 1, keyValue.length - 1);
                    return getDefaultQualifiedAttributeNameFromType(asContext.get(keyValue[0]), s);
                }
            }

            return getDefaultQualifiedAttributeNameFromType(activeType, item);
        }

        public String getDefaultQualifiedAttributeNameFromType(String s, String item) {
            return StringUtils.isEmpty(s) ? item : String.format("%s.%s", s, item);
        }

        @Override
        public String getTypeFromEdge(String item) {
            return "Column";
        }

        @Override
        public boolean isAttributePrimitiveTypeForActiveType(String s) {
            return s.equalsIgnoreCase("name") || s.equalsIgnoreCase("owner");
        }

        @Override
        public boolean isTypeTrait(String name) {
            return name.equalsIgnoreCase("PII");
        }

        public boolean doesActiveTypeHaveSubTypes() {
            return activeType.equalsIgnoreCase("Asset");
        }

        public String getActiveTypeAndSubTypes() {
            String[] str = new String[]{"'Asset'", "'Table'"};
            return StringUtils.join(str, ",");
        }

        @Override
        public boolean isSameAsActive(String typeName) {
            return (activeType != null) && activeType.equalsIgnoreCase(typeName);
        }
    }
}

