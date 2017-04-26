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
package org.apache.atlas.repository.typestore;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.TestOnlyModule;
import org.apache.atlas.util.AtlasRepositoryConfiguration;
import org.apache.commons.configuration.Configuration;


/**
 * Guice module which sets TypeCache implementation class configuration property to {@link StoreBackedTypeCache}.
 *
 */
public class StoreBackedTypeCacheTestOnlyModule extends TestOnlyModule {

    @Override
    protected Configuration getConfiguration() {
        try {
            Configuration configuration = ApplicationProperties.get();
            configuration.setProperty(AtlasRepositoryConfiguration.TYPE_CACHE_IMPLEMENTATION_PROPERTY,
                    StoreBackedTypeCache.class.getName());
            return configuration;
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }
    }
}
