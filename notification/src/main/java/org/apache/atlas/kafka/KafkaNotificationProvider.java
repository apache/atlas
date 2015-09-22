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

package org.apache.atlas.kafka;

import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.commons.configuration.Configuration;

public class KafkaNotificationProvider implements Provider<KafkaNotification> {
    @Override
    @Provides
    @Singleton
    public KafkaNotification get() {
        try {
            Configuration applicationProperties = ApplicationProperties.get();
            KafkaNotification instance = new KafkaNotification(applicationProperties);
            return instance;
        } catch(AtlasException e) {
            throw new RuntimeException(e);
        }
    }
}
