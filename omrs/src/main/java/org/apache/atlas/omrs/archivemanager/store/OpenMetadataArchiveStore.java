/*
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
package org.apache.atlas.omrs.archivemanager.store;


import org.apache.atlas.omrs.archivemanager.properties.OpenMetadataArchive;

/**
 * <p>
 * OpenMetadataArchiveStore is the interface for a connector to an open metadata archive.  The open metadata archive
 * is a collection of type definitions (TypeDefs) and metadata instances (Entities and Relationships) that can be
 * loaded into an open metadata repository.
 * </p>
 * <p>
 *     An open metadata archive has 3 sections:
 * </p>
 * <ul>
 *     <li>
 *         Archive properties
 *     </li>
 *     <li>
 *         Type store - ordered list of types
 *     </li>
 *     <li>
 *         Instance store - list of entities and relationships
 *     </li>
 * </ul>
 */
public interface OpenMetadataArchiveStore
{
    /**
     * Return the contents of the archive.
     *
     * @return OpenMetadataArchive object
     */
    OpenMetadataArchive getArchiveContents();


    /**
     * Set new contents into the archive.  This overrides any content previously stored.
     *
     * @param archiveContents - OpenMetadataArchive object
     */
    void setArchiveContents(OpenMetadataArchive   archiveContents);
}
