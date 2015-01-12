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

package org.apache.hadoop.metadata.service;

import java.io.Closeable;
import java.io.IOException;

/**
 * Service interface that's initialized at startup.
 */
public interface Service extends Closeable {

    /**
     * Starts the service. This method blocks until the service has completely started.
     *
     * @throws Exception
     */
    void start() throws Exception;

    /**
     * Stops the service. This method blocks until the service has completely shut down.
     */
    void stop();

    /**
     * A version of stop() that is designed to be usable in Java7 closure
     * clauses.
     * Implementation classes MUST relay this directly to {@link #stop()}
     * @throws java.io.IOException never
     * @throws RuntimeException on any failure during the stop operation
     */
    void close() throws IOException;
}
