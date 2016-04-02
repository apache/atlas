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

package org.apache.atlas.web.service;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import org.apache.atlas.listener.ActiveStateChangeHandler;
import org.apache.atlas.notification.NotificationHookConsumer;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.service.Service;
import org.apache.atlas.services.DefaultMetadataService;

/**
 * A Guice module that registers the handlers of High Availability state change handlers and other services.
 *
 * Any new handler that should react to HA state change should be registered here.
 */
public class ActiveInstanceElectorModule extends AbstractModule {
    @Override
    protected void configure() {
        Multibinder<ActiveStateChangeHandler> activeStateChangeHandlerBinder =
                Multibinder.newSetBinder(binder(), ActiveStateChangeHandler.class);
        activeStateChangeHandlerBinder.addBinding().to(GraphBackedSearchIndexer.class);
        activeStateChangeHandlerBinder.addBinding().to(DefaultMetadataService.class);
        activeStateChangeHandlerBinder.addBinding().to(NotificationHookConsumer.class);
        //Enable this after ATLAS-498 is committed
        //activeStateChangeHandlerBinder.addBinding().to(HBaseBasedAuditRepository.class);

        Multibinder<Service> serviceBinder = Multibinder.newSetBinder(binder(), Service.class);
        serviceBinder.addBinding().to(ActiveInstanceElectorService.class);
    }
}
