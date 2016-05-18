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

import com.tinkerpop.pipes.Pipe;
import org.apache.atlas.catalog.VertexWrapper;

/**
 * Query expression which always returns true.
 */
public class AlwaysQueryExpression extends BaseQueryExpression {
    protected AlwaysQueryExpression() {
        super(null, null, null);
    }

    @Override
    public Pipe asPipe() {
        return null;
    }

    @Override
    public boolean evaluate(VertexWrapper vWrapper) {
        return ! negate;
    }

    @Override
    public boolean evaluate(Object value) {
        return true;
    }
}
