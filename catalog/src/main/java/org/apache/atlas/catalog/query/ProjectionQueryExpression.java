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
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.filter.FilterFunctionPipe;
import org.apache.atlas.catalog.VertexWrapper;
import org.apache.atlas.catalog.definition.ResourceDefinition;
import org.apache.atlas.catalog.projection.ProjectionResult;
import org.apache.atlas.catalog.projection.Relation;

import java.util.*;

/**
 * Query expression wrapper which handles projection queries.
 */
public class ProjectionQueryExpression extends BaseQueryExpression {

    private final QueryExpression underlyingExpression;
    private final ResourceDefinition resourceDefinition;

    private final String[] fieldSegments;

    protected ProjectionQueryExpression(QueryExpression underlyingExpression, ResourceDefinition resourceDefinition) {
        super(underlyingExpression.getField(), underlyingExpression.getExpectedValue(), resourceDefinition);

        this.underlyingExpression = underlyingExpression;
        this.resourceDefinition = resourceDefinition;
        this.fieldSegments = getField().split(QueryFactory.PATH_SEP_TOKEN);
    }

    @Override
    public Pipe asPipe() {
        //todo: encapsulate all of this path logic including path sep escaping and normalizing
        final int sepIdx = getField().indexOf(QueryFactory.PATH_SEP_TOKEN);
        final String edgeToken = getField().substring(0, sepIdx);
        GremlinPipeline pipeline = new GremlinPipeline();

        Relation relation = resourceDefinition.getRelations().get(fieldSegments[0]);
        if (relation != null) {
            pipeline = pipeline.outE();
            pipeline.add(relation.asPipe()).inV();
        } else {
            if (resourceDefinition.getProjections().get(fieldSegments[0]) != null) {
                return super.asPipe();
            } else {
                //todo: default Relation implementation
                pipeline = pipeline.outE().has("label", Text.REGEX, String.format(".*\\.%s", edgeToken)).inV();
            }
        }
        //todo: set resource definition from relation on underlying expression where appropriate
        String childFieldName = getField().substring(sepIdx + QueryFactory.PATH_SEP_TOKEN.length());
        underlyingExpression.setField(childFieldName);

        Pipe childPipe;
        if (childFieldName.contains(QueryFactory.PATH_SEP_TOKEN)) {
            childPipe = new ProjectionQueryExpression(underlyingExpression, resourceDefinition).asPipe();
        } else {
            childPipe = underlyingExpression.asPipe();
        }
        pipeline.add(childPipe);

        return negate ? new FilterFunctionPipe(new ExcludePipeFunction(pipeline)) : pipeline;
    }

    @Override
    public boolean evaluate(VertexWrapper vWrapper) {
        boolean result = false;
        Iterator<ProjectionResult> projectionIterator = resourceDefinition.getProjections().
                get(fieldSegments[0]).values(vWrapper).iterator();

        while (! result && projectionIterator.hasNext()) {
            ProjectionResult projectionResult = projectionIterator.next();
            for (Map<String, Object> propertyMap : projectionResult.getPropertyMaps()) {
                Object val = propertyMap.get(fieldSegments[1]);
                if (val != null && underlyingExpression.evaluate(QueryFactory.escape(val))) {
                    result = true;
                    break;
                }
            }
        }
        return negate ^ result;
    }

    private static class ExcludePipeFunction implements PipeFunction<Object, Boolean> {
        private final GremlinPipeline excludePipeline;

        public ExcludePipeFunction(GremlinPipeline excludePipeline) {
            this.excludePipeline = excludePipeline;
        }

        @Override
        public Boolean compute(Object vertices) {
            GremlinPipeline p = new GremlinPipeline(Collections.singleton(vertices));
            p.add(excludePipeline);
            return p.gather().toList().isEmpty();
        }
    }

    protected QueryExpression getUnderlyingExpression() {
        return underlyingExpression;
    }
}
