/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.falcon.event;

import org.apache.falcon.entity.v0.Entity;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.Date;

/**
 * Falcon event to interface with Atlas Service.
 */
public class FalconEvent {
    protected String user;
    protected UserGroupInformation ugi;
    protected OPERATION operation;
    protected long timestamp;
    protected Entity entity;

    public FalconEvent(String doAsUser, UserGroupInformation ugi, OPERATION falconOperation, long timestamp, Entity entity) {
        this.user = doAsUser;
        this.ugi = ugi;
        this.operation = falconOperation;
        this.timestamp = timestamp;
        this.entity = entity;
    }

    public enum OPERATION {
        ADD_CLUSTER,
        UPDATE_CLUSTER,
        ADD_FEED,
        UPDATE_FEED,
        ADD_PROCESS,
        UPDATE_PROCESS,
    }

    public String getUser() {
        return user;
    }

    public UserGroupInformation getUgi() {
        return ugi;
    }

    public OPERATION getOperation() {
        return operation;
    }

    public Date getTimestamp() {
        return new Date(timestamp);
    }

    public Entity getEntity() {
        return entity;
    }
}
