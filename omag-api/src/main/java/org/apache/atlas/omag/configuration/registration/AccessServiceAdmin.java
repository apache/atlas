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
package org.apache.atlas.omag.configuration.registration;

import org.apache.atlas.ocf.properties.Connection;
import org.apache.atlas.omag.configuration.properties.AccessServiceConfig;
import org.apache.atlas.omag.ffdc.exception.OMAGConfigurationErrorException;

/**
 * AccessServiceAdmin is the interface that an access service implements to receive its configuration.
 * The java class that implements this interface is created with a default constructor and then
 * the initialize method is called.  It is configured in the AccessServiceDescription enumeration.
 */
public interface AccessServiceAdmin
{
    /**
     * Initialize the access service.
     *
     * @param configurationProperties - specific configuration properties for this access service.
     * @throws OMAGConfigurationErrorException - invalid parameters in the configuration properties.

     */
    void initialize(AccessServiceConfig configurationProperties,
                    Connection          enterpriseOMRSTopicConnector) throws OMAGConfigurationErrorException;


    /**
     * Refresh the configuration in the access service.
     *
     * @param configurationProperties - specific configuration properties for this access service.
     * @throws OMAGConfigurationErrorException - invalid parameters in the configuration properties.
     */
    void refreshConfiguration(AccessServiceConfig configurationProperties,
                              Connection          enterpriseOMRSTopicConnector) throws OMAGConfigurationErrorException;


    /**
     * Shutdown the access service.
     */
    void shutdown();
}
