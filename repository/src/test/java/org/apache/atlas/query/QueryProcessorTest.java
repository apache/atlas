/*
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
package org.apache.atlas.query;

import com.google.common.collect.ImmutableSet;
import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.cache.DefaultTypeCache;
import org.testng.annotations.Test;
import scala.util.Either;
import scala.util.parsing.combinator.Parsers;

import java.util.HashSet;
import java.util.Set;

import static org.apache.atlas.typesystem.types.utils.TypesUtil.createClassTypeDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createRequiredAttrDef;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Tests the logic for skipping type cache lookup for things that
 * cannot be types.
 *
 */
public class QueryProcessorTest {


    @Test
    public void testAliasesNotTreatedAsTypes() throws Exception {

        ValidatingTypeCache tc = findTypeLookupsDuringQueryParsing("hive_db as inst where inst.name=\"Reporting\" select inst as id, inst.name");
        assertTrue(tc.wasTypeRequested("hive_db"));
        assertFalse(tc.wasTypeRequested("inst"));
        assertFalse(tc.wasTypeRequested("name"));

    }


    @Test
    public void testFieldInComparisionNotTreatedAsType() throws Exception {

        //test when the IdExpression is on the left, on the right, and on both sides of the ComparsionExpression
        ValidatingTypeCache tc = findTypeLookupsDuringQueryParsing("hive_db where name=\"Reporting\" or \"Reporting\" = name or name=name");
        assertTrue(tc.wasTypeRequested("hive_db"));
        assertFalse(tc.wasTypeRequested("name"));

    }


    @Test
    public void testFieldInArithmeticExprNotTreatedAsType() throws Exception {

        //test when the IdExpression is on the left, on the right, and on both sides of the ArithmeticExpression
        ValidatingTypeCache tc = findTypeLookupsDuringQueryParsing("hive_db where (tableCount + 3) > (tableCount + tableCount) select (3 + tableCount) as updatedCount");

        assertTrue(tc.wasTypeRequested("hive_db"));
        assertFalse(tc.wasTypeRequested("tableCount"));
        assertFalse(tc.wasTypeRequested("updatedCount"));

    }

    @Test
    public void testFieldInSelectListWithAlasNotTreatedAsType() throws Exception {

        ValidatingTypeCache tc = findTypeLookupsDuringQueryParsing("hive_db select name as theName");
        assertTrue(tc.wasTypeRequested("hive_db"));
        assertFalse(tc.wasTypeRequested("theName"));
        assertFalse(tc.wasTypeRequested("name"));

    }

    @Test
    public void testFieldInSelectListNotTreatedAsType() throws Exception {


        ValidatingTypeCache tc = findTypeLookupsDuringQueryParsing("hive_db select name");
        assertTrue(tc.wasTypeRequested("hive_db"));
        assertFalse(tc.wasTypeRequested("name"));

    }

    private ValidatingTypeCache findTypeLookupsDuringQueryParsing(String query) throws AtlasException {
        TypeSystem typeSystem = TypeSystem.getInstance();
        ValidatingTypeCache result = new ValidatingTypeCache();
        typeSystem.setTypeCache(result);
        typeSystem.reset();
        HierarchicalTypeDefinition<ClassType> hiveTypeDef = createClassTypeDef("hive_db", "", ImmutableSet.<String>of(),
                createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                createRequiredAttrDef("tableCount", DataTypes.INT_TYPE)
        );
        typeSystem.defineClassType(hiveTypeDef);

        Either<Parsers.NoSuccess, Expressions.Expression> either = QueryParser.apply(query, null);
        Expressions.Expression expression = either.right().get();

        QueryProcessor.validate(expression);

        return result;
    }

    private static class ValidatingTypeCache extends DefaultTypeCache {

        private Set<String> typesRequested = new HashSet<>();

        @Override
        public boolean has(String typeName) throws AtlasException {
            typesRequested.add(typeName);
            return super.has(typeName);
        }

        @Override
        public boolean has(TypeCategory typeCategory, String typeName) throws AtlasException {
            typesRequested.add(typeName);
            return super.has(typeCategory, typeName);
        }

        @Override
        public IDataType get(String typeName) throws AtlasException {
            typesRequested.add(typeName);
            return super.get(typeName);
        }

        @Override
        public IDataType get(TypeCategory typeCategory, String typeName) throws AtlasException {
            typesRequested.add(typeName);
            return super.get(typeCategory, typeName);
        }

        public boolean wasTypeRequested(String name) {
            return typesRequested.contains(name);
        }
    }



}
