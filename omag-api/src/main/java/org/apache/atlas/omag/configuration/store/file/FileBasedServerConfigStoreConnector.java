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
package org.apache.atlas.omag.configuration.store.file;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.ocf.properties.Connection;
import org.apache.atlas.ocf.properties.Endpoint;
import org.apache.atlas.omag.configuration.properties.OMAGServerConfig;
import org.apache.atlas.omag.configuration.store.OMAGServerConfigStoreConnectorBase;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class FileBasedServerConfigStoreConnector extends OMAGServerConfigStoreConnectorBase
{
    /*
     * This is the name of the configuration file that is used if there is no file name in the connection.
     */
    private static final String defaultFilename = "omag.server.config";

    /*
     * Variables used in writing to the file.
     */
    private String           configStoreName  = null;

    /*
     * Variables used for logging and debug.
     */
    private static final Logger log = LoggerFactory.getLogger(FileBasedServerConfigStoreConnector.class);


    /**
     * Default constructor
     */
    public FileBasedServerConfigStoreConnector()
    {
    }


    @Override
    public void initialize(String connectorInstanceId, Connection connection)
    {
        super.initialize(connectorInstanceId, connection);

        Endpoint endpoint = connection.getEndpoint();

        if (endpoint != null)
        {
            configStoreName = endpoint.getAddress();
        }

        if (configStoreName == null)
        {
            configStoreName = defaultFilename;
        }
    }


    /**
     * Save the server configuration.
     *
     * @param omagServerConfig - configuration properties to save
     */
    public void saveServerConfig(OMAGServerConfig omagServerConfig)
    {
        File    configStoreFile = new File(configStoreName);

        try
        {
            if (log.isDebugEnabled())
            {
                log.debug("Writing server config store properties", omagServerConfig);
            }

            if (omagServerConfig == null)
            {
                configStoreFile.delete();
            }
            else
            {
                ObjectMapper objectMapper = new ObjectMapper();

                String configStoreFileContents = objectMapper.writeValueAsString(omagServerConfig);

                FileUtils.writeStringToFile(configStoreFile, configStoreFileContents, false);
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
     * Retrieve the configuration saved from a previous run of the server.
     *
     * @return server configuration
     */
    public OMAGServerConfig  retrieveServerConfig()
    {
        File             configStoreFile     = new File(configStoreName);
        OMAGServerConfig newConfigProperties;

        try
        {
            if (log.isDebugEnabled())
            {
                log.debug("Retrieving server configuration properties");
            }

            String configStoreFileContents = FileUtils.readFileToString(configStoreFile, "UTF-8");

            ObjectMapper objectMapper = new ObjectMapper();

            newConfigProperties = objectMapper.readValue(configStoreFileContents, OMAGServerConfig.class);
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

            newConfigProperties = new OMAGServerConfig();
        }

        return newConfigProperties;
    }


    /**
     * Remove the server configuration.
     */
    public void removeServerConfig()
    {
        File    configStoreFile = new File(configStoreName);

        configStoreFile.delete();
    }


    /**
     * Close the config file
     */
    public void disconnect()
    {
        if (log.isDebugEnabled())
        {
            log.debug("Closing Config Store.");
        }
    }
}
