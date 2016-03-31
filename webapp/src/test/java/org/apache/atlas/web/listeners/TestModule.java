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

package org.apache.atlas.web.listeners;

import com.google.inject.Binder;
import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.repository.audit.EntityAuditRepository;
import org.apache.atlas.repository.audit.InMemoryEntityAuditRepository;

public class TestModule extends RepositoryMetadataModule {
    @Override
    protected void bindAuditRepository(Binder binder) {
        //Map EntityAuditRepository interface to hbase based implementation
        binder.bind(EntityAuditRepository.class).to(InMemoryEntityAuditRepository.class).asEagerSingleton();
    }
}
