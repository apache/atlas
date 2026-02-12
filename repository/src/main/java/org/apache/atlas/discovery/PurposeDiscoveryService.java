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
import org.apache.atlas.model.discovery.PurposeUserRequest;
import org.apache.atlas.model.discovery.PurposeUserResponse;

/**
 * Service interface for discovering Purpose entities accessible to users.
 * This service provides optimized queries for fetching purposes based on
 * user and group memberships, eliminating the need for frontend aggregation
 * of AuthPolicy entities.
 */
public interface PurposeDiscoveryService {

    /**
     * Discover all Purpose entities accessible to a user based on their
     * username and group memberships.
     * <p>
     * This method queries AuthPolicy entities with policyCategory="purpose"
     * that match the user's username or group memberships, then aggregates
     * and returns the unique Purpose entities referenced by those policies.
     * </p>
     *
     * @param request The request containing username, groups, and pagination parameters
     * @return Response containing accessible Purpose entities with pagination info
     * @throws AtlasBaseException if discovery fails due to query errors or invalid parameters
     */
    PurposeUserResponse discoverPurposesForUser(PurposeUserRequest request) throws AtlasBaseException;
}
