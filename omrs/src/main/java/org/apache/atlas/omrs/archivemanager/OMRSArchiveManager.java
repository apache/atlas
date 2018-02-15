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
package org.apache.atlas.omrs.archivemanager;

import org.apache.atlas.omrs.archivemanager.opentypes.OpenMetadataTypesArchive;
import org.apache.atlas.omrs.archivemanager.store.OpenMetadataArchiveStore;
import org.apache.atlas.omrs.archivemanager.properties.OpenMetadataArchive;
import org.apache.atlas.omrs.archivemanager.properties.OpenMetadataArchiveInstanceStore;
import org.apache.atlas.omrs.archivemanager.properties.OpenMetadataArchiveProperties;
import org.apache.atlas.omrs.archivemanager.properties.OpenMetadataArchiveTypeStore;
import org.apache.atlas.omrs.auditlog.OMRSAuditLog;
import org.apache.atlas.omrs.auditlog.OMRSAuditingComponent;
import org.apache.atlas.omrs.eventmanagement.events.OMRSInstanceEventProcessor;
import org.apache.atlas.omrs.eventmanagement.events.OMRSTypeDefEventProcessor;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryContentManager;
import org.apache.atlas.omrs.metadatacollection.properties.instances.EntityDetail;
import org.apache.atlas.omrs.metadatacollection.properties.instances.Relationship;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDef;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDefPatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * OMRSArchiveManager manages the loading and unloading of open metadata archives from the local OMRS repository.
 * An open metadata archive provides pre-built definitions for types and metadata instances.
 */
public class OMRSArchiveManager
{
    ArrayList<OpenMetadataArchiveStore> openMetadataArchiveStores   = new ArrayList<>();
    OMRSRepositoryContentManager        repositoryContentManager    = null;
    OMRSInstanceEventProcessor          localInstanceEventProcessor = null;


    /*
     * The audit log provides a verifiable record of the open metadata archives that have been loaded into
     * the open metadata repository.  The Logger is for standard debug.
     */
    private static final OMRSAuditLog auditLog = new OMRSAuditLog(OMRSAuditingComponent.ARCHIVE_MANAGER);
    private static final Logger       log      = LoggerFactory.getLogger(OMRSArchiveManager.class);


    /**
     * Constructor to save the initial list of open metadata archives from the server startup configuration.
     * These will be processed as soon as the event processors are supplied from the local repository.
     *
     * @param startUpOpenMetadataArchives - initial list of open metadata archives provided in startup configuration
     */
    public OMRSArchiveManager(ArrayList<OpenMetadataArchiveStore>    startUpOpenMetadataArchives)
    {
        if (startUpOpenMetadataArchives != null)
        {
            for (OpenMetadataArchiveStore archiveStore : startUpOpenMetadataArchives)
            {
                this.openMetadataArchiveStores.add(archiveStore);
            }
        }
    }


    /**
     * Close down any open archives.
     */
    public void close()
    {
        for (OpenMetadataArchiveStore    archiveStore : openMetadataArchiveStores)
        {
            if (archiveStore != null)
            {
                archiveStore.closeArchive();
            }
        }
    }


    /**
     * The local repository is accessed through its inbound event processors.  A server will always have
     * the local Content Manager and TypeDef Processor but the local Instance Processor is only available
     * if the local server has a metadata repository defined.
     *
     * @param repositoryContentManager - typeDef processor for the local repository
     * @param instanceProcessor - instance processor for the local repository
     */
    public void setLocalRepository(OMRSRepositoryContentManager repositoryContentManager,
                                   OMRSInstanceEventProcessor   instanceProcessor)
    {
        this.repositoryContentManager = repositoryContentManager;
        this.localInstanceEventProcessor = instanceProcessor;

        /*
         * The repository content manager is seeded with all of the open metadata types.
         */
        processOpenMetadataTypes();

        /*
         * Once the open metadata types are in place, the archive stores are processed.
         */
        for (OpenMetadataArchiveStore archiveStore : this.openMetadataArchiveStores)
        {
            processOpenMetadataArchiveStore(archiveStore, repositoryContentManager, instanceProcessor);
        }
    }


    /**
     * Add a new archive to the local repository. If there are problems processing the archive, an exception is thrown.
     * This method allows archives to be loaded into a running server.
     *
     * @param archiveStore - new open metadata archive to process
     */
    public void addOpenMetadataArchive(OpenMetadataArchiveStore     archiveStore)
    {
        this.processOpenMetadataArchiveStore(archiveStore, repositoryContentManager, localInstanceEventProcessor);
        this.openMetadataArchiveStores.add(archiveStore);
    }


    /**
     * Unpack and process the contents an open metadata archive store, passing its contents to the local
     * repository (if it exists).
     */
    private void processOpenMetadataTypes()
    {
        OpenMetadataTypesArchive    openMetadataTypesArchive = new OpenMetadataTypesArchive();
        OpenMetadataArchive         openMetadataTypes        = openMetadataTypesArchive.getOpenMetadataArchive();

        repositoryContentManager.setOpenMetadataTypesOriginGUID(openMetadataTypesArchive.getArchiveGUID());
        processOpenMetadataArchive(openMetadataTypes, repositoryContentManager, localInstanceEventProcessor);
    }

    /**
     * Unpack and process the contents an open metadata archive store, passing its contents to the local
     * repository (if it exists).
     *
     * @param archiveStore - open metadata archive store to process
     */
    private void processOpenMetadataArchiveStore(OpenMetadataArchiveStore     archiveStore,
                                                 OMRSTypeDefEventProcessor    typeDefProcessor,
                                                 OMRSInstanceEventProcessor   instanceProcessor)
    {
        if (archiveStore != null)
        {
            /*
             * Each archive store has a header, a section of new type definitions (TypeDefs) and a section of
             * metadata instances.
             */
            OpenMetadataArchive archiveContent = archiveStore.getArchiveContents();

            if (archiveContent == null)
            {
                // TODO log Error
            }
            else
            {
                processOpenMetadataArchive(archiveContent, typeDefProcessor, instanceProcessor);
            }
        }
        else
        {
            // TODO log empty Archive store
        }
    }


    /**
     * Step through the content of an open metadata archive, passing its contents to the local repository (if it
     * exists).
     *
     * @param archiveContent - open metadata archive to process
     */
    private void processOpenMetadataArchive(OpenMetadataArchive          archiveContent,
                                            OMRSTypeDefEventProcessor    typeDefProcessor,
                                            OMRSInstanceEventProcessor   instanceProcessor)
    {
        OpenMetadataArchiveProperties    archiveProperties    = archiveContent.getArchiveProperties();
        OpenMetadataArchiveTypeStore     archiveTypeStore     = archiveContent.getArchiveTypeStore();
        OpenMetadataArchiveInstanceStore archiveInstanceStore = archiveContent.getArchiveInstanceStore();

        // TODO Log that the open metadata archive is being processed

        if (archiveProperties != null)
        {
            if (archiveTypeStore != null)
            {
                this.processTypeDefStore(archiveProperties, archiveTypeStore, typeDefProcessor);
            }

            if (archiveInstanceStore != null)
            {
                this.processInstanceStore(archiveProperties, archiveInstanceStore, instanceProcessor);
            }
        }
        else
        {
            // TODO Log that the archive is invalid
        }


        // TODO log that processing complete
    }


    /**
     * The TypeStore is in two parts.  First there is an optional list of patches to existing TypeDefs.
     * Then an optional list of new TypeDefs.  It is possible that this archive has been processed before
     * and so any duplicates detected are ignored.  However, conflicting TypeDefs are detected.
     * Any problems found in applying the archive contents are recorded on the audit log.
     *
     * @param archiveProperties - properties of the archive used for logging
     * @param archiveTypeStore - TypeStore from the archive
     */
    private void processTypeDefStore(OpenMetadataArchiveProperties    archiveProperties,
                                     OpenMetadataArchiveTypeStore     archiveTypeStore,
                                     OMRSTypeDefEventProcessor        typeDefProcessor)
    {
        ArrayList<TypeDefPatch>   typeDefPatches = archiveTypeStore.getTypeDefPatches();
        ArrayList<TypeDef>        newTypeDefs    = archiveTypeStore.getNewTypeDefs();

        if (typeDefProcessor != null)
        {
            String       sourceName = OMRSAuditingComponent.ARCHIVE_MANAGER.getComponentName();
            String       originatorMetadataCollectionId = archiveProperties.getArchiveGUID();
            String       originatorServerName = archiveProperties.getArchiveName();
            String       originatorServerType = null;
            String       originatorOrganizationName = archiveProperties.getOriginatorName();

            if (archiveProperties.getArchiveType() != null)
            {
                originatorServerType = archiveProperties.getArchiveType().getArchiveTypeName();
            }

            if (typeDefPatches != null)
            {
                for (TypeDefPatch typeDefPatch : typeDefPatches)
                {
                    if (typeDefPatch != null)
                    {
                        typeDefProcessor.processUpdatedTypeDefEvent(sourceName,
                                                                    originatorMetadataCollectionId,
                                                                    originatorServerName,
                                                                    originatorServerType,
                                                                    originatorOrganizationName,
                                                                    typeDefPatch);
                    }
                }
            }

            if (newTypeDefs != null)
            {
                for (TypeDef newTypeDef : newTypeDefs)
                {
                    if (newTypeDef != null)
                    {
                        typeDefProcessor.processNewTypeDefEvent(sourceName,
                                                                originatorMetadataCollectionId,
                                                                originatorServerName,
                                                                originatorServerType,
                                                                originatorOrganizationName,
                                                                newTypeDef);
                    }
                }
            }
        }
    }


    /**
     * The InstanceStore is in two parts - an optional list of entities followed by an optional list
     * of relationships.  It is possible that this archive has been processed before
     * and so any duplicates detected are ignored.  However, conflicting instances are detected.
     * Any problems found in applying the archive contents are recorded on the audit log.
     *
     * @param archiveProperties - properties describing the archive used in logging
     * @param archiveInstanceStore - the instance store to process
     * @param instanceProcessor - the processor to add the instances to the local repository.  It may be null
     *                          if there is no local repository configured for this server.
     */
    private void processInstanceStore(OpenMetadataArchiveProperties    archiveProperties,
                                      OpenMetadataArchiveInstanceStore archiveInstanceStore,
                                      OMRSInstanceEventProcessor       instanceProcessor)
    {
        ArrayList<EntityDetail>    entities = archiveInstanceStore.getEntities();
        ArrayList<Relationship>    relationships = archiveInstanceStore.getRelationships();

        if (instanceProcessor != null)
        {
            String       sourceName = OMRSAuditingComponent.ARCHIVE_MANAGER.getComponentName();
            String       originatorMetadataCollectionId = archiveProperties.getArchiveGUID();
            String       originatorServerName = archiveProperties.getArchiveName();
            String       originatorServerType = null;
            String       originatorOrganizationName = archiveProperties.getOriginatorName();

            if (archiveProperties.getArchiveType() != null)
            {
                originatorServerType = archiveProperties.getArchiveType().getArchiveTypeName();
            }

            if (entities != null)
            {
                for (EntityDetail entity : entities)
                {
                    if (entity != null)
                    {
                        instanceProcessor.processNewEntityEvent(sourceName,
                                                                originatorMetadataCollectionId,
                                                                originatorServerName,
                                                                originatorServerType,
                                                                originatorOrganizationName,
                                                                entity);
                    }
                }
            }


            if (relationships != null)
            {
                for (Relationship relationship : relationships)
                {
                    if (relationship != null)
                    {
                        instanceProcessor.processNewRelationshipEvent(sourceName,
                                                                      originatorMetadataCollectionId,
                                                                      originatorServerName,
                                                                      originatorServerType,
                                                                      originatorOrganizationName,
                                                                      relationship);
                    }
                }
            }
        }
    }
}
