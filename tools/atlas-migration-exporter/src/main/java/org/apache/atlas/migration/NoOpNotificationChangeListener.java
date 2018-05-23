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

package org.apache.atlas.migration;

import org.apache.atlas.AtlasException;
import org.apache.atlas.listener.EntityChangeListener;
import org.apache.atlas.model.glossary.AtlasGlossaryTerm;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.atlas.v1.model.instance.Struct;
import org.springframework.stereotype.Component;

import java.util.Collection;

@Component
public class NoOpNotificationChangeListener implements EntityChangeListener {
    @Override
    public void onEntitiesAdded(Collection<Referenceable> entities, boolean isImport) throws AtlasException {

    }

    @Override
    public void onEntitiesUpdated(Collection<Referenceable> entities, boolean isImport) throws AtlasException {

    }

    @Override
    public void onTraitsAdded(Referenceable entity, Collection<? extends Struct> traits) throws AtlasException {

    }

    @Override
    public void onTraitsDeleted(Referenceable entity, Collection<? extends Struct> traits) throws AtlasException {

    }

    @Override
    public void onTraitsUpdated(Referenceable entity, Collection<? extends Struct> traits) throws AtlasException {

    }

    @Override
    public void onEntitiesDeleted(Collection<Referenceable> entities, boolean isImport) throws AtlasException {

    }

    @Override
    public void onTermAdded(Collection<Referenceable> entities, AtlasGlossaryTerm term) throws AtlasException {

    }

    @Override
    public void onTermDeleted(Collection<Referenceable> entities, AtlasGlossaryTerm term) throws AtlasException {

    }
}
