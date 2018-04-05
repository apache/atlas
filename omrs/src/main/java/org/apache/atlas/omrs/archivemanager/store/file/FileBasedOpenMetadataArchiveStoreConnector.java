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
package org.apache.atlas.omrs.archivemanager.store.file;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.ocf.ffdc.ConnectorCheckedException;
import org.apache.atlas.ocf.properties.Connection;
import org.apache.atlas.ocf.properties.Endpoint;
import org.apache.atlas.omrs.archivemanager.properties.OpenMetadataArchive;
import org.apache.atlas.omrs.archivemanager.store.OpenMetadataArchiveStoreConnector;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class FileBasedOpenMetadataArchiveStoreConnector extends OpenMetadataArchiveStoreConnector
{
    /*
     * This is the default name of the open metadata archive file that is used if there is no file name in the connection.
     */
    private static final String defaultFilename = "open.metadata.archive";

    /*
     * Variables used in writing to the file.
     */
    private String archiveStoreName = null;

    /*
     * Variables used for logging and debug.
     */
    private static final Logger log = LoggerFactory.getLogger(FileBasedOpenMetadataArchiveStoreConnector.class);


    /**
     * Default constructor
     */
    public FileBasedOpenMetadataArchiveStoreConnector()
    {
    }


    @Override
    public void initialize(String connectorInstanceId, Connection connection)
    {
        super.initialize(connectorInstanceId, connection);

        Endpoint endpoint = connection.getEndpoint();

        if (endpoint != null)
        {
            archiveStoreName = endpoint.getAddress();
        }

        if (archiveStoreName == null)
        {
            archiveStoreName = defaultFilename;
        }
    }


    /**
     * Return the contents of the archive.
     *
     * @return OpenMetadataArchive object
     */
    public OpenMetadataArchive getArchiveContents()
    {
        File                archiveStoreFile     = new File(archiveStoreName);
        OpenMetadataArchive newOpenMetadataArchive;

        try
        {
            if (log.isDebugEnabled())
            {
                log.debug("Retrieving server configuration properties");
            }

            String configStoreFileContents = FileUtils.readFileToString(archiveStoreFile, "UTF-8");

            ObjectMapper objectMapper = new ObjectMapper();

            newOpenMetadataArchive = objectMapper.readValue(configStoreFileContents, OpenMetadataArchive.class);
        }
        catch (IOException ioException)
        {
            /*
             * The config file is not found, create a new one ...
             */

            if (log.isDebugEnabled())
            {
                log.debug("New server config Store", ioException);
            }

            newOpenMetadataArchive = new OpenMetadataArchive();
        }

        return newOpenMetadataArchive;
    }


    /**
     * Set new contents into the archive.  This overrides any content previously stored.
     *
     * @param archiveContents - OpenMetadataArchive object
     */
    public void setArchiveContents(OpenMetadataArchive   archiveContents)
    {
        File    archiveStoreFile = new File(archiveStoreName);

        try
        {
            if (log.isDebugEnabled())
            {
                log.debug("Writing open metadata archive store properties: " + archiveContents);
            }

            if (archiveContents == null)
            {
                archiveStoreFile.delete();
            }
            else
            {
                ObjectMapper objectMapper = new ObjectMapper();

                String archiveStoreFileContents = objectMapper.writeValueAsString(archiveContents);

                FileUtils.writeStringToFile(archiveStoreFile, archiveStoreFileContents, false);
            }
        }
        catch (IOException   ioException)
        {
            if (log.isDebugEnabled())
            {
                log.debug("Unusable Server config Store :(", ioException);
            }
        }
    }


    /**
     * Indicates that the connector is completely configured and can begin processing.
     *
     * @throws ConnectorCheckedException - there is a problem within the connector.
     */
    public void start() throws ConnectorCheckedException
    {
        super.start();
    }


    /**
     * Free up any resources held since the connector is no longer needed.
     *
     * @throws ConnectorCheckedException - there is a problem within the connector.
     */
    public  void disconnect() throws ConnectorCheckedException
    {
        super.disconnect();

        if (log.isDebugEnabled())
        {
            log.debug("Closing Config Store.");
        }
    }
}
