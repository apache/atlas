/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.discovery;


import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;

public interface AtlasDiscoveryService {
    /**
     *
     * @param query search query in DSL format.
     * @param limit number of resultant rows (for pagination). [ limit > 0 ] and [ limit < maxlimit ]. -1 maps to atlas.search.defaultlimit property.
     * @param offset offset to the results returned (for pagination). [ offset >= 0 ]. -1 maps to offset 0.
     * @return AtlasSearchResult
     */
    AtlasSearchResult searchUsingDslQuery(String query, int limit, int offset) throws AtlasBaseException;

    /**
     *
     * @param query search query.
     * @param limit number of resultant rows (for pagination). [ limit > 0 ] and [ limit < maxlimit ]. -1 maps to atlas.search.defaultlimit property.
     * @param offset offset to the results returned (for pagination). [ offset >= 0 ]. -1 maps to offset 0.
     * @return AtlasSearchResult
     */
    AtlasSearchResult searchUsingFullTextQuery(String query, int limit, int offset) throws AtlasBaseException;
}
