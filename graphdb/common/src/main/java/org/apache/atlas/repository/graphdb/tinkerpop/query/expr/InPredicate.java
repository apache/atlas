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

import org.apache.atlas.repository.graphdb.tinkerpop.query.NativeTinkerpopGraphQuery;

import java.util.Collection;

/**
 * Query predicate that checks whether the value of a given property is within the
 * provided set of allowed values.
 */
public class InPredicate implements QueryPredicate {
    private final String        propertyName;
    private final Collection<?> values;

    public InPredicate(String propertyName, Collection<?> values) {
        super();

        this.propertyName = propertyName;
        this.values       = values;
    }

    @Override
    public void addTo(NativeTinkerpopGraphQuery<?, ?> query) {
        query.in(propertyName, values);
    }

    @Override
    public String toString() {
        return "InPredicate [propertyName=" + propertyName + ", values=" + values + "]";
    }
}
