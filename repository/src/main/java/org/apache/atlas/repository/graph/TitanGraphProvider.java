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

package org.apache.atlas.repository.graph;

import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.graphdb.titan0.Titan0Database;
import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.core.TitanGraph;

/**
 * Temporary TitanGraphProvider to use until the graph database abstraction
 * layer is fully in place.  Delegates to the Titan 0.5.4 implementation.  This
 * will be removed once the abstraction layer is being used.
 */
public class TitanGraphProvider implements GraphProvider<TitanGraph> {

    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graph.GraphProvider#get()
     */
    @Override
    public TitanGraph get() {
        return Titan0Database.getGraphInstance();
    }

    public static TitanGraph getGraphInstance() {
        return Titan0Database.getGraphInstance();
    }

    public static Configuration getConfiguration() throws AtlasException {
        return Titan0Database.getConfiguration();
    }

}
