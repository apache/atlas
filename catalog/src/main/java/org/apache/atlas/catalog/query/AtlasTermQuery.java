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

import com.thinkaurelius.titan.core.attribute.Text;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.Pipe;
import org.apache.atlas.catalog.Request;
import org.apache.atlas.catalog.TermPath;
import org.apache.atlas.catalog.definition.ResourceDefinition;
import org.apache.atlas.repository.Constants;

/**
 * Term resource query.
 */
public class AtlasTermQuery extends BaseQuery {
    private final TermPath termPath;

    public AtlasTermQuery(QueryExpression queryExpression, ResourceDefinition resourceDefinition, TermPath termPath, Request request) {
        super(queryExpression, resourceDefinition, request);
        this.termPath = termPath;
    }

    @Override
    protected Pipe getQueryPipe() {
        return new GremlinPipeline().has("Taxonomy.name", termPath.getTaxonomyName()).out().
                has(Constants.ENTITY_TYPE_PROPERTY_KEY, Text.PREFIX, termPath.getFullyQualifiedName());
    }
}
