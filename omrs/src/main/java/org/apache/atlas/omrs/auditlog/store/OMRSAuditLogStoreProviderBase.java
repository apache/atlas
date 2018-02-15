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
package org.apache.atlas.omrs.auditlog.store;

import org.apache.atlas.ocf.ConnectorProviderBase;

/**
 * The OMRSAuditLogStoreProviderBase provides a base class for the connector provider supporting OMRS
 * audit log stores.  It extends ConnectorProviderBase which does the creation of connector instances.
 * The subclasses of OMRSAuditLogStoreProviderBase must initialize ConnectorProviderBase with the Java class
 * name of the audit log connector implementation (by calling super.setConnectorClassName(className)).
 * Then the connector provider will work.
 */
public abstract class OMRSAuditLogStoreProviderBase extends ConnectorProviderBase
{
    /**
     * Default Constructor
     */
    public OMRSAuditLogStoreProviderBase()
    {
        /*
         * Nothing to do
         */
    }
}

