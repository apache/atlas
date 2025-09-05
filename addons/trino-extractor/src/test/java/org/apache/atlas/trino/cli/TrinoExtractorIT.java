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
package org.apache.atlas.trino.cli;

public class TrinoExtractorIT {
 /* List of testcases
    Invalid Arguments
    Invalid cron expression
    Test valid Catalog to be run
    Test Instance creation
    Test catalog creation
    Test schema creation
    Test table creation
    Test of hook is enabled, hook entity if created, is connected to Trino entity
    Test cron doesn't trigger new job, before earlier thread completes
    Test without cron expression
    Test even if catalog is not registered, it should run if passed from commandLine
    Deleted table
    Deleted catalog
    Deleted column
    Deleted schema
    Rename catalog
    Rename schema
    Tag propagated*/
}
