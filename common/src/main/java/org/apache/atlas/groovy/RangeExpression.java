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

package org.apache.atlas.groovy;

/**
 * Represents an "exclusive" range expression, e.g. [0..&lt;10].
 */
public class RangeExpression extends AbstractGroovyExpression {

    private GroovyExpression parent;
    private int offset;
    private int count;

    public RangeExpression(GroovyExpression parent, int offset, int count) {
        this.parent = parent;
        this.offset = offset;
        this.count = count;
    }

    @Override
    public void generateGroovy(GroovyGenerationContext context) {
        parent.generateGroovy(context);
        context.append(" [");
        new LiteralExpression(offset).generateGroovy(context);
        context.append("..<");
        new LiteralExpression(count).generateGroovy(context);
        context.append("]");
    }
}
