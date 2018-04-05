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


import org.apache.atlas.ocf.Connector;
import org.apache.atlas.ocf.ConnectorBroker;
import org.apache.atlas.ocf.properties.Connection;
import org.apache.atlas.omrs.admin.OMRSConfigurationFactory;
import org.apache.atlas.omrs.archivemanager.opentypes.OpenMetadataTypesArchive;
import org.apache.atlas.omrs.archivemanager.properties.OpenMetadataArchive;
import org.apache.atlas.omrs.archivemanager.store.OpenMetadataArchiveStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OMRSArchiveWriter creates physical open metadata archive files for the supplied open metadata archives
 * encoded in OMRS.
 */
public class OMRSArchiveWriter
{
    private static final Logger log = LoggerFactory.getLogger(OMRSArchiveWriter.class);

    /**
     * Default constructor
     */
    public OMRSArchiveWriter()
    {
    }


    /**
     * Opens up an open metadata archive store connector.
     *
     * @param connection - connection information for the open metadata archive.
     * @return open metadata archive store connector
     */
    private OpenMetadataArchiveStore getOpenMetadataArchive(Connection   connection)
    {
        OpenMetadataArchiveStore  openMetadataArchiveStore = null;

        try
        {
            ConnectorBroker connectorBroker = new ConnectorBroker();
            Connector       connector       = connectorBroker.getConnector(connection);

            openMetadataArchiveStore = (OpenMetadataArchiveStore)connector;

            log.debug("Created connector to open metadata archive store");

        }
        catch (Throwable   error)
        {
            log.error("Unexpected exception occurred: " + error.getMessage());
            log.error("Exception: " + error.toString());
        }

        return openMetadataArchiveStore;
    }


    /**
     * Generates and writes out an open metadata archive containing all of the open metadata types.
     */
    private void writeOpenMetadataArchiveTypes()
    {
        OMRSConfigurationFactory  configurationFactory     = new OMRSConfigurationFactory();
        Connection                connection               = configurationFactory.getOpenMetadataTypesConnection();

        OpenMetadataArchiveStore  openMetadataArchiveStore = this.getOpenMetadataArchive(connection);
        OpenMetadataTypesArchive  openMetadataTypesArchive = new OpenMetadataTypesArchive();
        OpenMetadataArchive       openMetadataArchive      = openMetadataTypesArchive.getOpenMetadataArchive();

        openMetadataArchiveStore.setArchiveContents(openMetadataArchive);
    }


    /**
     * Main program to control the archive writer.
     *
     * @param args - ignored arguments
     */
    public static void main(String[] args)
    {
        OMRSArchiveWriter  archiveWriter = new OMRSArchiveWriter();

        archiveWriter.writeOpenMetadataArchiveTypes();

        /*
         * Calls to create other standard archives will go here.
         */
    }
}
