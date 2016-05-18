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

import java.util.Collection;

/**
 * Represents a query expression.
 */
public interface QueryExpression {
    /**
     * Evaluate the expression based on properties of the provied vertex.
     *
     * @param vWrapper vertex wrapper that expression is applied to
     * @return result of expression evaluation
     */
    boolean evaluate(VertexWrapper vWrapper);

    /**
     * Evaluate the expression based on the provided value.
     *
     * @param value  value used to evaluate expression
     * @return
     */
    boolean evaluate(Object value);

    /**
     * Get the complete set of properties which are contained in the expression.
     *
     * @return collection of expression properties
     */
    Collection<String> getProperties();

    /**
     * Get the pipe representation of the expression.
     *
     * @return pipe representation
     */
    Pipe asPipe();

    /**
     * Negate the expression.
     */
    void setNegate();

    /**
     * Get the negate status of the expression.
     *
     * @return true if the expression is negated, false otherwise
     */
    boolean isNegate();

    /**
     * Determine whether the expression is being applied to a projection.
     *
     * @return true if expression is being applied to a projection, false otherwise
     */
    boolean isProjectionExpression();

    /**
     * Get the field name used in the expression.
     *
     * @return expression field name or null if there is no field name
     */
    String getField();

    /**
     * Set the expressions field name.
     *
     * @param fieldName  field name
     */
    public void setField(String fieldName);

    /**
     * Get the expected value for the expression.
     *
     * @return expected value or null if there isn't a expected value
     */
    String getExpectedValue();
}
