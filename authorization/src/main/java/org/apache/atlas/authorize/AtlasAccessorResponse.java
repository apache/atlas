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
package org.apache.atlas.authorize;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.HashSet;
import java.util.Set;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class AtlasAccessorResponse {

    private String action;

    private String guid;
    private String typeName;
    private String qualifiedName;
    private String label;
    private String classification;
    private String businessMetadata;

    private String relationshipTypeName;
    private String entityGuidEnd1;
    private String entityQualifiedNameEnd1;
    private String entityTypeEnd1;
    private String entityGuidEnd2;
    private String entityQualifiedNameEnd2;
    private String entityTypeEnd2;

    private Set<String> users = new HashSet<>();
    private Set<String> groups = new HashSet<>();
    private Set<String> roles = new HashSet<>();

    public AtlasAccessorResponse() {

    }

    public void populateRequestDetails(AtlasAccessorRequest accessorRequest) {
        this.action = accessorRequest.getAction();
        this.guid = accessorRequest.getGuid();
        this.typeName = accessorRequest.getTypeName();
        this.qualifiedName = accessorRequest.getQualifiedName();
        this.label = accessorRequest.getLabel();
        this.classification = accessorRequest.getClassification();
        this.businessMetadata = accessorRequest.getBusinessMetadata();

        this.relationshipTypeName = accessorRequest.getRelationshipTypeName();
        this.entityGuidEnd1 = accessorRequest.getEntityGuidEnd1();
        this.entityQualifiedNameEnd1 = accessorRequest.getEntityQualifiedNameEnd1();
        this.entityTypeEnd1 = accessorRequest.getEntityTypeEnd1();
        this.entityGuidEnd2 = accessorRequest.getEntityGuidEnd2();
        this.entityQualifiedNameEnd2 = accessorRequest.getEntityQualifiedNameEnd2();
        this.entityTypeEnd2 = accessorRequest.getEntityTypeEnd2();
    }

    public String getAction() {
        return action;
    }

    public String getGuid() {
        return guid;
    }

    public String getTypeName() {
        return typeName;
    }

    public String getQualifiedName() {
        return qualifiedName;
    }

    public String getLabel() {
        return label;
    }

    public String getClassification() {
        return classification;
    }

    public String getBusinessMetadata() {
        return businessMetadata;
    }

    public String getRelationshipTypeName() {
        return relationshipTypeName;
    }

    public String getEntityGuidEnd1() {
        return entityGuidEnd1;
    }

    public String getEntityQualifiedNameEnd1() {
        return entityQualifiedNameEnd1;
    }

    public String getEntityTypeEnd1() {
        return entityTypeEnd1;
    }

    public String getEntityGuidEnd2() {
        return entityGuidEnd2;
    }

    public String getEntityQualifiedNameEnd2() {
        return entityQualifiedNameEnd2;
    }

    public String getEntityTypeEnd2() {
        return entityTypeEnd2;
    }

    public Set<String> getUsers() {
        return users;
    }

    public void setUsers(Set<String> users) {
        this.users = users;
    }

    public Set<String> getGroups() {
        return groups;
    }

    public void setGroups(Set<String> groups) {
        this.groups = groups;
    }

    public Set<String> getRoles() {
        return roles;
    }

    public void setRoles(Set<String> roles) {
        this.roles = roles;
    }

    @Override
    public String toString() {
        return "AtlasAccessorResponse{" +
                "guid='" + guid + '\'' +
                ", typeName='" + typeName + '\'' +
                ", qualifiedName='" + qualifiedName + '\'' +
                ", label='" + label + '\'' +
                ", classification='" + classification + '\'' +
                ", businessMetadata='" + businessMetadata + '\'' +
                ", users=" + users +
                ", groups=" + groups +
                ", roles=" + roles +
                '}';
    }
}
