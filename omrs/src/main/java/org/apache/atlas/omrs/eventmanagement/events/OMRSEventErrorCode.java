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
package org.apache.atlas.omrs.eventmanagement.events;


/**
 * OMRSEventErrorCode is a merging of the OMRSRegistryEventErrorCode, OMRSTypeDefEventErrorCode and
 * OMRSInstanceEventErrorCode that is used in OMRSEvent.  Detailed description of the values can be found
 * in the source enums.
 */
public enum OMRSEventErrorCode
{
    CONFLICTING_COLLECTION_ID,
    CONFLICTING_TYPEDEFS,
    CONFLICTING_ATTRIBUTE_TYPEDEFS,
    CONFLICTING_INSTANCES,
    CONFLICTING_TYPE,
    BAD_REMOTE_CONNECTION,
    TYPEDEF_PATCH_MISMATCH,
    INVALID_EVENT_FORMAT,
    INVALID_REGISTRY_EVENT,
    INVALID_TYPEDEF_EVENT,
    INVALID_INSTANCE_EVENT
}
