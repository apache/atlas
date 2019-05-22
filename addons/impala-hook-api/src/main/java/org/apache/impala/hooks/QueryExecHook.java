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
package org.apache.impala.hooks;


/**
 * {@link QueryExecHook} is the interface for implementations that
 * can hook into supported places in Impala query execution.
 */
public interface QueryExecHook {

    /**
     * Hook method invoked when the Impala daemon starts up.
     * <p>
     * This method will block completion of daemon startup, so you should
     * execute any long-running actions asynchronously.
     * </p>
     * <h3>Error-Handling</h3>
     * <p>
     * Any {@link Exception} thrown from this method will effectively fail
     * Impala startup with an error. Implementations should handle all
     * exceptions as gracefully as they can, even if the end result is to
     * throw them.
     * </p>
     */
    void impalaStartup();

    /**
     * Hook method invoked asynchronously when a (qualifying) Impala query
     * has executed, but before it has returned.
     * <p>
     * This method will not block the invoking or subsequent queries,
     * but may block future hook invocations if it runs for too long
     * </p>
     * <h3>Error-Handling</h3>
     * <p>
     * Any {@link Exception} thrown from this method will only be caught
     * and logged and will not affect the result of any query.
     * </p>
     *
     * @param context object containing the post execution context
     *                of the query
     */
    void postQueryExecute(PostQueryHookContext context);
}