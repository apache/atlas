<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~     http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->

# AssetConsumer Open Metadata Access Service (OMAS)

The AssetConsumer OMAS provides services for an application
accessing assets such as data stores, APIs or functions such as analytical services.

The AssetConsumer REST API supports the retrieval of connection metadata, the
adding of feedback to specific assets and an audit log for the asset.

The AssetConsumer Java client supports all of the operations of the REST API.
It adds the capability to act as a factory for connectors to assets.
The Java client takes the name or id of a connection, looks up the properties
of the connection and, using the Open Connector Framework (OCF), it creates a new
connector instance and returns it to the caller.

In addition it can add and remove feedback (tags, ratings, comments, likes) from
the asset description.

The caller can use the connector to access metadata about the
asset it is accessing.   This service is provided by the ConnectedAsset OMAS.

