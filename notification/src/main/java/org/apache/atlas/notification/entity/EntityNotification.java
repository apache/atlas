/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.notification.entity;

import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.IStruct;

import java.util.List;

/**
 * Notification of entity changes.
 */
public interface EntityNotification {

    /**
     * Operations that result in an entity notification.
     */
    enum OperationType {
        ENTITY_CREATE,
        ENTITY_UPDATE,
        TRAIT_ADD,
        TRAIT_DELETE
    }


    // ----- EntityNotification ------------------------------------------------

    /**
     * Get the entity that is associated with this notification.
     *
     * @return the associated entity
     */
    IReferenceableInstance getEntity();

    /**
     * Get flattened list of traits that are associated with this entity (includes super traits).
     *
     * @return the list of all traits
     */
    List<IStruct> getAllTraits();

    /**
     * Get the type of operation that triggered this notification.
     *
     * @return the operation type
     */
    OperationType getOperationType();
}
