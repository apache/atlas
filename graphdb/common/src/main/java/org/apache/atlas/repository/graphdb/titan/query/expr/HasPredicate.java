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
package org.apache.atlas.repository.graphdb.titan.query.expr;

import org.apache.atlas.repository.graphdb.AtlasGraphQuery.ComparisionOperator;
import org.apache.atlas.repository.graphdb.titan.query.NativeTitanGraphQuery;

/**
 * Query predicate that checks whether the given property has the specified
 * relationship with the value specified.
 */
public class HasPredicate implements QueryPredicate {

    private String propertyName_;
    private ComparisionOperator op_;
    private Object value_;

    public HasPredicate(String propertyName, ComparisionOperator op, Object value) {
        super();
        propertyName_ = propertyName;
        op_ = op;
        value_ = value;
    }

    @Override
    public void addTo(NativeTitanGraphQuery query) {
        query.has(propertyName_, op_, value_);
    }

    @Override
    public String toString() {
        return "HasTerm [propertyName_=" + propertyName_ + ", op_=" + op_ + ", value_=" + value_ + "]";
    }

}