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

# Connected Asset Open Metadata Access Service (OMAS)

The Connected Asset OMAS implements the ConnectedAssetProperties API that is
available on every **OCF connector**.

An OCF connector is a connector that supports the open connector framework (OCF).
It has 3 sets of APIs:
* An API to return properties about the connector and its connection
* An API to access the asset it connects to
* An API to access the metadata about the asset the connector is used to access

The Connected Asset OMAS is the third API on an OCF connector - the one for the metadata about the asset.
It is a generic API for all types of open metadata assets.  However, it assumes the
asset's metadata model inherits from **Asset** (see model 0010 in Area 0).

The Connected Asset OMAS returns metadata about the asset at three levels of detail:

* getAssetSummary - returns the summary information organized in the assetSummary structure.
* getAssetDetail - returns detailed information about the asset organized in the assetDetail structure.
* getAssetUniverse - returns all of the common metadata properties connected to the asset such as its schma, meanings
and platform.

These structures are defined in the OCF module as POJO property objects.

