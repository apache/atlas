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
package org.apache.atlas.query;

import org.apache.atlas.query.Expressions.Expression;


public class GremlinTranslator {
    private final QueryProcessor queryProcessor;
    private       Expression     expression;

    public GremlinTranslator(QueryProcessor queryProcessor, Expression expression) {
        this.expression     = expression;
        this.queryProcessor = queryProcessor;
    }

    public GremlinQuery translate() {
        DSLVisitor qv = new DSLVisitor(queryProcessor);

        expression.accept(qv);
        queryProcessor.close();

        GremlinQuery ret = new GremlinQuery(queryProcessor.getText(), queryProcessor.hasSelect());
        return ret;
    }
}
