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

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.filter.FilterFunctionPipe;
import org.apache.atlas.catalog.VertexWrapper;
import org.apache.atlas.catalog.definition.ResourceDefinition;

import java.util.Collection;
import java.util.HashSet;

/**
 * Base query expression class.
 */
public abstract class BaseQueryExpression implements QueryExpression {
    protected String m_field;
    protected final String m_expectedValue;
    protected final ResourceDefinition resourceDefinition;
    protected boolean negate = false;
    protected Collection<String> properties = new HashSet<>();

    protected BaseQueryExpression(String field, String expectedValue, ResourceDefinition resourceDefinition) {
        m_field = field;
        if (field != null) {
            properties.add(field);
        }
        m_expectedValue = expectedValue;
        this.resourceDefinition = resourceDefinition;
    }

    @Override
    public boolean evaluate(VertexWrapper vWrapper) {
        return negate ^ evaluate(vWrapper.getProperty(m_field));
    }

    @Override
    public Collection<String> getProperties() {
        return properties;
    }

    @Override
    public boolean evaluate(Object value) {
        // subclasses which don't override evaluate(VertexWrapper) should implement this
        return false;
    }

    //todo: use 'has' instead of closure where possible for performance
    public Pipe asPipe() {
        return new FilterFunctionPipe(new PipeFunction<Vertex, Boolean>() {
            @Override
            public Boolean compute(Vertex vertex) {
                return evaluate(new VertexWrapper(vertex, resourceDefinition));
            }
        });
    }

    @Override
    public String getField() {
        return m_field;
    }

    @Override
    public String getExpectedValue() {
        return m_expectedValue;
    }

    @Override
    public void setField(String field) {
        m_field = field;
    }

    @Override
    public void setNegate() {
        this.negate = true;
    }

    @Override
    public boolean isNegate() {
        return negate;
    }

    @Override
    public boolean isProjectionExpression() {
        return getField() != null && getField().contains(QueryFactory.PATH_SEP_TOKEN);
    }
}
