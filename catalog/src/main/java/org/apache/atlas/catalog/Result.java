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

package org.apache.atlas.catalog;

import java.util.Collection;
import java.util.Map;

/**
 * Resource provider result.
 */
public class Result {
    /**
     * collection of property maps
     */
    private Collection<Map<String, Object>> propertyMaps;

    /**
     * Constructor.
     *
     * @param propertyMaps collection of property maps
     */
    public Result(Collection<Map<String, Object>> propertyMaps) {
        this.propertyMaps = propertyMaps;
    }

    /**
     * Obtain the result property maps.
     *
     * @return result property maps
     */
    public Collection<Map<String, Object>> getPropertyMaps() {
        return propertyMaps;
    }
}
