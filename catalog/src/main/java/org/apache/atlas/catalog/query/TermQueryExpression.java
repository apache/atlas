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

import org.apache.atlas.catalog.definition.ResourceDefinition;
import org.apache.lucene.search.TermQuery;

import java.util.Collection;

/**
 * Query expression which evaluates whether a property equals a value.
 */
public class TermQueryExpression extends BaseQueryExpression {

    public TermQueryExpression(TermQuery query, ResourceDefinition resourceDefinition) {
        super(query.getTerm().field(), query.getTerm().text(), resourceDefinition);
    }

    @Override
    public boolean evaluate(Object value) {
        String expectedValue = getExpectedValue();
        if (value == null) {
            return expectedValue.equals("null");
        //todo: refactor; we shouldn't need to use instanceof/cast here
        } else if (value instanceof Collection) {
            return ((Collection)value).contains(expectedValue);
        } else {
            return expectedValue.equals(QueryFactory.escape(String.valueOf(value)));
        }
    }

    public String getExpectedValue() {
        return m_expectedValue;
    }

}
