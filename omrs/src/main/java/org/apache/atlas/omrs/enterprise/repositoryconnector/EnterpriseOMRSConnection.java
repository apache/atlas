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

package org.apache.atlas.omrs.enterprise.repositoryconnector;

import org.apache.atlas.ocf.properties.Connection;
import org.apache.atlas.ocf.properties.ConnectorType;

/**
 * EnterpriseOMRSConnection provides a valid connection for the EnterpriseOMRSConnector.
 */
public class EnterpriseOMRSConnection extends Connection
{
    final ConnectorType enterpriseConnectorType = new ConnectorType(null,
                                                                    null,
                                                                    null,
                                                                    null,
                                                                    null,
                                                                    null,
                                                                    null,
                                                                    null,
                                                                    null,
                                                                    EnterpriseOMRSConnectorProvider.class.getName());


    /**
     * Default Constructor that sets up the connector
     */
    public EnterpriseOMRSConnection()
    {
        super(null);
        super.connectorType = enterpriseConnectorType;
    }
}
