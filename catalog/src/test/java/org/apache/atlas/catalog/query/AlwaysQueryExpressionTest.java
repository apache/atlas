/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.catalog.query;

import org.apache.atlas.catalog.VertexWrapper;
import org.testng.annotations.Test;

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for AlwaysQueryExpression.
 */
public class AlwaysQueryExpressionTest {
    @Test
    public void testEvaluate() {
        VertexWrapper v = createStrictMock(VertexWrapper.class);
        replay(v);
        QueryExpression expression = new AlwaysQueryExpression();
        // always returns true
        assertTrue(expression.evaluate(v));
        verify(v);
    }

    @Test
    public void testEvaluate_negated() {
        VertexWrapper v = createStrictMock(VertexWrapper.class);
        replay(v);
        QueryExpression expression = new AlwaysQueryExpression();
        expression.setNegate();
        // always returns true
        assertFalse(expression.evaluate(v));
        assertTrue(expression.isNegate());
        verify(v);
    }

    @Test
    public void testGetProperties() {
        VertexWrapper v = createStrictMock(VertexWrapper.class);
        replay(v);
        QueryExpression expression = new AlwaysQueryExpression();
        assertTrue(expression.getProperties().isEmpty());
        verify(v);
    }

    @Test
    public void testAsPipe() {
        VertexWrapper v = createStrictMock(VertexWrapper.class);
        replay(v);
        QueryExpression expression = new AlwaysQueryExpression();
        assertNull(expression.asPipe());
        verify(v);
    }
}
