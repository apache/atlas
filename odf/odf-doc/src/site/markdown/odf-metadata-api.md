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

# ODF Metadata API

ODF provides a very simple API for searching, retrieving, and (to a limited extent) for creating
new metadata objects.
This API abstracts away specifics of the underlying metadata store.
See the REST resource `metadata`, e.g., [here](https://sdp1.rtp.raleigh.ibm.com:58081/odf-web-0.1.0-SNAPSHOT/swagger/#/metadata) or look at the Java interface `org.apache.atlas.odf.core.metadata.MetadataStore`.

In this API we distinguish between `MetaDataObject`s and `MetadataObjectReferences`.
Where the former represent an object as such, the latter is just a reference to an object.
You may think of the `MetaDataObjectReference` as a generalized XMeta RID.

Simply put, metadata objects are represented as JSON where object attributes
are represented as JSON attribute with the same name.
Simple types map to JSON simple types. References
to another object are represented of JSON objects of type `MetadataObjectReference` that
has three attribute:
1. `id`: the object ID
2. `repositoryId`: the ID of the repository where the object resides
3. `url` (optional): A URL pointing to the object. For Atlas, this is a link to the object in the
Atlas dashboard.

The API is read-only, the only objects that can be created are annotations (see section [Data model and extensibility](data-model.html).

Here is an example: suppose there is a table object which has a name and a list of columns. The JSON of this table would look something like this:

	{
	   "name": "CUSTOMERS,
	   "columns": [
	                 {
	                    "id": "1234-abcd",
	                    "repositoryId": "atlas:repos1"
	                 },
	                 {
	                    "id": "5678-efgh",
	                    "repositoryId": "atlas:repos1"
	                 }
	              ],
	   "reference": {
	                  "id": "9abc-ijkl",
	                  "repositoryId": "atlas:repos1"
	                },
	   "javaClass": "corg.apache.atlas.odf.core.metadata.models.Table"              
	}

The `reference` value represent the reference to the object itself where as
`javaClass` denotes the type of object (table in this case).
The `name` attribute contains the table name where the `columns` value is a list
of references to two column objects. These references can be retrieved separately
to look at the details.
