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
package org.apache.atlas.tasks;

/**
 * Raised when another node already holds the claim being taken.
 *
 * <p>This is an expected outcome, not a failure: exactly one node wins each claim and every other
 * node is told so by this exception.  It is deliberately unchecked and allowed to propagate out of
 * the claiming method so that the surrounding {@code @GraphTransaction} rolls back — the store has
 * already rejected the write, and on the rdbms backend nothing further can be done in that
 * transaction anyway.  Callers should route claim attempts through
 * {@link GraphClaim#attempt(GraphClaim.ClaimAttempt)} rather than catching this directly, since
 * some backends only report the conflict once the transaction commits.
 */
public class ClaimConflictException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final String claimName;

    public ClaimConflictException(String claimName, Throwable cause) {
        super("claim '" + claimName + "' is held by another node", cause);

        this.claimName = claimName;
    }

    public String getClaimName() {
        return claimName;
    }
}
