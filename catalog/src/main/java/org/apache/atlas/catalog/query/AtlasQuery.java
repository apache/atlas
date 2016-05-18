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

import org.apache.atlas.catalog.exception.ResourceNotFoundException;

import java.util.Collection;
import java.util.Map;

/**
 * Query functionality.
 */
public interface AtlasQuery {
    /**
     * Execute the query.
     *
     * @return collection of property maps, one per matching resource
     * @throws ResourceNotFoundException if an explicitly specified resource doesn't exist
     */
    Collection<Map<String, Object>> execute() throws ResourceNotFoundException;
}
