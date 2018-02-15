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
package org.apache.atlas.omrs.metadatahighway.cohortregistry.store.properties;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;
import java.util.ArrayList;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;

/**
 * CohortMembership describes the structure of the cohort registry store.  It contains details
 * of the local registration and a list of remote member registrations.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class CohortMembership implements Serializable
{
    private static final long serialVersionUID = 1L;

    private MemberRegistration            localRegistration   = null;
    private ArrayList<MemberRegistration> remoteRegistrations = null;

    public CohortMembership()
    {
    }

    public MemberRegistration getLocalRegistration()
    {
        return localRegistration;
    }

    public void setLocalRegistration(MemberRegistration localRegistration)
    {
        this.localRegistration = localRegistration;
    }

    public ArrayList<MemberRegistration> getRemoteRegistrations()
    {
        return remoteRegistrations;
    }

    public void setRemoteRegistrations(ArrayList<MemberRegistration> remoteRegistrations)
    {
        this.remoteRegistrations = remoteRegistrations;
    }
}
