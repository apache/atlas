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

package org.apache.hadoop.metadata.repository.graph;

public class GraphServiceConfigurator extends PropertyBasedConfigurator<GraphService> {
    private static final String PROPERTY_NAME = "metadata.graph.impl.class";
    private static final String DEFAULT_IMPL_CLASS = TitanGraphService.class.getName();
    private static final String CONFIG_PATH = "application.properties";

    public GraphServiceConfigurator() {
        super("metadata.graph.propertyName", "metadata.graph.defaultImplClass",
                "metadata.graph.configurationPath", PROPERTY_NAME,
                DEFAULT_IMPL_CLASS, CONFIG_PATH);
    }
}
