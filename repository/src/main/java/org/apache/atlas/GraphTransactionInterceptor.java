/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas;

import com.google.inject.Inject;
import com.thinkaurelius.titan.core.TitanGraph;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.atlas.repository.graph.GraphProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphTransactionInterceptor implements MethodInterceptor {
    private static final Logger LOG = LoggerFactory.getLogger(GraphTransactionInterceptor.class);
    private TitanGraph titanGraph;

    @Inject
    GraphProvider<TitanGraph> graphProvider;

    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {
        if (titanGraph == null) {
            titanGraph = graphProvider.get();
        }

        try {
            Object response = invocation.proceed();
            titanGraph.commit();
            LOG.info("graph commit");
            return response;
        } catch (Throwable t) {
            titanGraph.rollback();
            LOG.error("graph rollback due to exception ", t);
            throw t;
        }
    }
}
