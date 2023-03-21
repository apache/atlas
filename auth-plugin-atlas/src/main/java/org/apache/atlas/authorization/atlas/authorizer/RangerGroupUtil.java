package org.apache.atlas.authorization.atlas.authorizer;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.atlas.plugin.util.RangerUserStore;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class RangerGroupUtil {
    private long                     UserStoreVersion;
    private Map<String, Set<String>>       userGroupMapping;

    public  enum  GROUP_FOR {USER}

    public RangerGroupUtil(RangerUserStore userStore) {
        if (userStore != null) {
            UserStoreVersion = userStore.getUserStoreVersion();
            userGroupMapping = userStore.getUserGroupMapping();
        } else {
            UserStoreVersion = -1L;
        }
    }

    public void setUserStore(RangerUserStore userStore) {
		this.userGroupMapping = userStore.getUserGroupMapping();
        this.UserStoreVersion = userStore.getUserStoreVersion();
    }

    public long getUserStoreVersion() { return UserStoreVersion; }

    public Set<String> getContainedGroups(String userName) {
        Set<String> data = new LinkedHashSet<String>();   
        Set<String> tmpData = new LinkedHashSet<String>();   
        tmpData = userGroupMapping.get(userName);
        if (tmpData != null){
           data = tmpData;
        }
        return data;
    }
  
}


