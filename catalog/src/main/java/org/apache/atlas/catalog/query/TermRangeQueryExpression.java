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
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;

/**
 * Query expression which evaluates whether a property value is within a range.
 */
//todo: for month and year which are expressed via a single digit, must ensure that
//todo: a leading '0' is provided.  For example, "2016-1-5" must be converted to "2016-01-05".
//todo: Month and day values aren't currently validated.
public class TermRangeQueryExpression extends BaseQueryExpression {
    private final BytesRef m_lowerTerm;
    private final BytesRef m_upperTerm;
    private final boolean m_lowerInclusive;
    private final boolean m_upperInclusive;

    public TermRangeQueryExpression(TermRangeQuery query, ResourceDefinition resourceDefinition) {
        super(query.getField(), null, resourceDefinition);
        m_lowerTerm = query.getLowerTerm();
        m_upperTerm = query.getUpperTerm();
        m_lowerInclusive = query.includesLower();
        m_upperInclusive = query.includesUpper();
    }

    @Override
    public boolean evaluate(Object value) {
        BytesRef valueBytes = new BytesRef(String.valueOf(value));
       return compareLowerBound(valueBytes) && compareUpperBound(valueBytes);
    }

    private boolean compareLowerBound(BytesRef valueBytes) {
        return m_lowerTerm == null || (m_lowerInclusive ? valueBytes.compareTo(m_lowerTerm) > 0 :
                valueBytes.compareTo(m_lowerTerm) >= 0);
    }

    private boolean compareUpperBound(BytesRef valueBytes) {
        return m_upperTerm == null || (m_upperInclusive ? valueBytes.compareTo(m_upperTerm) < 0 :
                valueBytes.compareTo(m_upperTerm) <= 0);
    }

}
