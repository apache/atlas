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
package org.apache.atlas.repository.graphdb.tinkerpop.query.expr;

import org.apache.atlas.repository.graphdb.AtlasGraphQuery.QueryOperator;
import org.apache.atlas.repository.graphdb.tinkerpop.query.NativeTinkerpopGraphQuery;

/**
 * Query predicate that checks whether the given property has the specified
 * relationship with the value specified.
 */
public class HasPredicate implements QueryPredicate {
    private final String        propertyName;
    private final QueryOperator op;
    private final Object        value;

    public HasPredicate(String propertyName, QueryOperator op, Object value) {
        this.propertyName = propertyName;
        this.op           = op;
        this.value        = value;
    }

    @Override
    public void addTo(NativeTinkerpopGraphQuery<?, ?> query) {
        query.has(propertyName, op, value);
    }

    @Override
    public String toString() {
        return "HasTerm [propertyName=" + propertyName + ", op=" + op + ", value=" + value + "]";
    }
}
