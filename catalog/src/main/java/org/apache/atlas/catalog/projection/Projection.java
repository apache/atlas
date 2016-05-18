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

package org.apache.atlas.catalog.projection;

import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;
import org.apache.atlas.catalog.VertexWrapper;

import java.util.Collection;
import java.util.Collections;

/**
 * Projection representation.
 * Used to project properties onto a resource from another source.
 */
public class Projection {
    public enum Cardinality {SINGLE, MULTIPLE}

    private final String m_name;
    private final Cardinality m_cardinality;
    protected Pipeline<VertexWrapper, Collection<ProjectionResult>> m_pipeline = new Pipeline<>();

    public Projection(String name, Cardinality cardinality) {
        m_name = name;
        m_cardinality = cardinality;
    }

    public Projection(String name, Cardinality cardinality, Pipe<VertexWrapper, Collection<ProjectionResult>> pipe) {
        m_name = name;
        m_cardinality = cardinality;
        m_pipeline.addPipe(pipe);
    }

    public Collection<ProjectionResult> values(VertexWrapper start) {
        m_pipeline.setStarts(Collections.singleton(start));
        return m_pipeline.iterator().next();
    }

    public void addPipe(Pipe<Collection<ProjectionResult>, Collection<ProjectionResult>> p) {
        m_pipeline.addPipe(p);
    }

    public String getName() {
        return m_name;
    }

    public Cardinality getCardinality() {
        return m_cardinality;
    }
}
