/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.util;

import org.apache.atlas.query.QueryParams;

/**
 * Represents a key for an entry in the compiled query cache.
 *
 */
public class CompiledQueryCacheKey {

    private final String dslQuery;
    private final QueryParams queryParams;

    public CompiledQueryCacheKey(String dslQuery, QueryParams queryParams) {
        super();
        this.dslQuery = dslQuery;
        this.queryParams = queryParams;
    }

    public CompiledQueryCacheKey(String dslQuery) {
        super();
        this.dslQuery = dslQuery;
        this.queryParams = null;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((dslQuery == null) ? 0 : dslQuery.hashCode());
        result = prime * result + ((queryParams == null) ? 0 : queryParams.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {

        if (this == obj) {
            return true;
        }

        if (obj == null) {
            return false;
        }
        if (!(obj instanceof CompiledQueryCacheKey)) {
            return false;
        }

        CompiledQueryCacheKey other = (CompiledQueryCacheKey) obj;
        if (! equals(dslQuery, other.dslQuery)) {
            return false;
        }

        if (! equals(queryParams, other.queryParams)) {
            return false;
        }

        return true;
    }

    private static boolean equals(Object o1, Object o2) {
        if(o1 == o2) {
            return true;
        }
        if(o1 == null) {
            return o2 == null;
        }
        return o1.equals(o2);
    }
}