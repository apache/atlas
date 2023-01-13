/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.ranger;


import java.util.LinkedHashMap;
import java.util.List;

public class RangerUser {

    long id;
    String owner;
    String updatedBy;
    String name;
    String firstName;
    String lastName;
    String emailAddress;
    String description;
    int status;
    int isVisible;
    int userSource;

    List<String> groupIdList;
    List<String> groupNameList;
    List<String> userRoleList;

    public RangerUser() {

    }

    public RangerUser(LinkedHashMap userMap) {
        this.name = (String) userMap.get("name");
        this.id = (long) userMap.get("id");
        this.owner = (String) userMap.get("owner");
        this.updatedBy = (String) userMap.get("updatedBy");
        this.firstName = (String) userMap.get("firstName");
        this.lastName = (String) userMap.get("lastName");
        this.emailAddress = (String) userMap.get("emailAddress");
        this.description = (String) userMap.get("description");
        this.status = (int) userMap.get("status");
        this.isVisible = (int) userMap.get("isVisible");
        this.userSource = (int) userMap.get("userSource");

        this.groupIdList = (List<String>) userMap.get("groupIdList");
        this.groupNameList = (List<String>) userMap.get("groupNameList");
        this.userRoleList = (List<String>) userMap.get("userRoleList");
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getUpdatedBy() {
        return updatedBy;
    }

    public void setUpdatedBy(String updatedBy) {
        this.updatedBy = updatedBy;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getEmailAddress() {
        return emailAddress;
    }

    public void setEmailAddress(String emailAddress) {
        this.emailAddress = emailAddress;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public int getIsVisible() {
        return isVisible;
    }

    public void setIsVisible(int isVisible) {
        this.isVisible = isVisible;
    }

    public int getUserSource() {
        return userSource;
    }

    public void setUserSource(int userSource) {
        this.userSource = userSource;
    }

    public List<String> getGroupIdList() {
        return groupIdList;
    }

    public void setGroupIdList(List<String> groupIdList) {
        this.groupIdList = groupIdList;
    }

    public List<String> getGroupNameList() {
        return groupNameList;
    }

    public void setGroupNameList(List<String> groupNameList) {
        this.groupNameList = groupNameList;
    }

    public List<String> getUserRoleList() {
        return userRoleList;
    }

    public void setUserRoleList(List<String> userRoleList) {
        this.userRoleList = userRoleList;
    }

    @Override
    public String toString() {
        return "RangerUser{" +
                "id=" + id +
                ", owner='" + owner + '\'' +
                ", updatedBy='" + updatedBy + '\'' +
                ", name='" + name + '\'' +
                ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", emailAddress='" + emailAddress + '\'' +
                ", description='" + description + '\'' +
                ", status=" + status +
                ", isVisible=" + isVisible +
                ", userSource=" + userSource +
                ", groupIdList=" + groupIdList +
                ", groupNameList=" + groupNameList +
                ", userRoleList=" + userRoleList +
                '}';
    }
}
