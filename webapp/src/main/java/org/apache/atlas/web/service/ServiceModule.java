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

package org.apache.atlas.web.service;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import org.apache.atlas.kafka.KafkaNotification;
import org.apache.atlas.listener.EntityChangeListener;
import org.apache.atlas.notification.NotificationHookConsumer;
import org.apache.atlas.notification.NotificationEntityChangeListener;
import org.apache.atlas.service.Service;

public class ServiceModule extends AbstractModule {
    @Override
    protected void configure() {
        Multibinder<Service> serviceBinder = Multibinder.newSetBinder(binder(), Service.class);
        serviceBinder.addBinding().to(KafkaNotification.class);
        serviceBinder.addBinding().to(NotificationHookConsumer.class);

        //Add NotificationEntityChangeListener as EntityChangeListener
        Multibinder<EntityChangeListener> entityChangeListenerBinder =
                Multibinder.newSetBinder(binder(), EntityChangeListener.class);
        entityChangeListenerBinder.addBinding().to(NotificationEntityChangeListener.class);
    }
}
