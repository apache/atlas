/** Licensed to the Apache Software Foundation (ASF) under one
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
package org.apache.atlas.authorize.simple;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class AtlasSimpleAuthzPolicy implements Serializable {
    private static final long serialVersionUID = 1L;

    private Map<String, AtlasAuthzRole> roles;
    private Map<String, List<String>>   userRoles;
    private Map<String, List<String>>   groupRoles;


    public Map<String, AtlasAuthzRole> getRoles() {
        return roles;
    }

    public void setRoles(Map<String, AtlasAuthzRole> roles) {
        this.roles = roles;
    }

    public Map<String, List<String>> getUserRoles() {
        return userRoles;
    }

    public void setUserRoles(Map<String, List<String>> userRoles) {
        this.userRoles = userRoles;
    }

    public Map<String, List<String>> getGroupRoles() {
        return groupRoles;
    }

    public void setGroupRoles(Map<String, List<String>> groupRoles) {
        this.groupRoles = groupRoles;
    }


    @JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown=true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.PROPERTY)
    public static class AtlasAuthzRole implements Serializable {
        private static final long serialVersionUID = 1L;

        private List<AtlasAdminPermission>  adminPermissions;
        private List<AtlasEntityPermission> entityPermissions;
        private List<AtlasTypePermission>   typePermissions;

        public AtlasAuthzRole() {
        }

        public AtlasAuthzRole(List<AtlasAdminPermission> adminPermissions, List<AtlasEntityPermission> entityPermissions, List<AtlasTypePermission> typePermissions) {
            this.adminPermissions  = adminPermissions;
            this.entityPermissions = entityPermissions;
            this.typePermissions   = typePermissions;
        }

        public List<AtlasAdminPermission> getAdminPermissions() {
            return adminPermissions;
        }

        public void setAdminPermissions(List<AtlasAdminPermission> adminPermissions) {
            this.adminPermissions = adminPermissions;
        }

        public List<AtlasEntityPermission> getEntityPermissions() {
            return entityPermissions;
        }

        public void setEntityPermissions(List<AtlasEntityPermission> entityPermissions) {
            this.entityPermissions = entityPermissions;
        }

        public List<AtlasTypePermission> getTypePermissions() {
            return typePermissions;
        }

        public void setTypePermissions(List<AtlasTypePermission> typePermissions) {
            this.typePermissions = typePermissions;
        }
    }

    @JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown=true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.PROPERTY)
    public static class AtlasAdminPermission implements Serializable {
        private static final long serialVersionUID = 1L;

        private List<String> privileges; // name of AtlasPrivilege enum, wildcards supported

        public AtlasAdminPermission() {
        }

        public AtlasAdminPermission(List<String> privileges) {
            this.privileges = privileges;
        }

        public List<String> getPrivileges() {
            return privileges;
        }

        public void setPrivileges(List<String> privileges) {
            this.privileges = privileges;
        }
    }

    @JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown=true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.PROPERTY)
    public static class AtlasTypePermission implements Serializable {
        private static final long serialVersionUID = 1L;

        private List<String> privileges;     // name of AtlasPrivilege enum, wildcards supported
        private List<String> typeCategories; // category of the type (entity, classification, struct, enum, relationship), wildcards supported
        private List<String> typeNames;      // name of type, wildcards supported

        public AtlasTypePermission() {
        }

        public AtlasTypePermission(List<String> privileges, List<String> typeCategories, List<String> typeNames) {
            this.privileges     = privileges;
            this.typeCategories = typeCategories;
            this.typeNames      = typeNames;
        }

        public List<String> getPrivileges() {
            return privileges;
        }

        public void setPrivileges(List<String> privileges) {
            this.privileges = privileges;
        }

        public List<String> getTypeCategories() {
            return typeCategories;
        }

        public void setTypeCategories(List<String> typeCategories) {
            this.typeCategories = typeCategories;
        }

        public List<String> getTypeNames() {
            return typeNames;
        }

        public void setTypeNames(List<String> typeNames) {
            this.typeNames = typeNames;
        }
    }

    @JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown=true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.PROPERTY)
    public static class AtlasEntityPermission implements Serializable {
        private static final long serialVersionUID = 1L;

        private List<String> privileges;      // name of AtlasPrivilege enum, wildcards supported
        private List<String> entityTypes;     // name of entity-type, wildcards supported
        private List<String> entityIds;       // value of entity-unique attribute, wildcards supported
        private List<String> classifications; // name of classification-type, wildcards supported
        private List<String> attributes;      // name of entity-attribute, wildcards supported

        public AtlasEntityPermission() {
        }

        public AtlasEntityPermission(List<String> privileges, List<String> entityTypes, List<String> entityIds, List<String> classifications, List<String> attributes) {
            this.privileges      = privileges;
            this.entityTypes     = entityTypes;
            this.entityIds       = entityIds;
            this.classifications = classifications;
            this.attributes      = attributes;
        }

        public List<String> getPrivileges() {
            return privileges;
        }

        public void setPrivileges(List<String> privileges) {
            this.privileges = privileges;
        }

        public List<String> getEntityTypes() {
            return entityTypes;
        }

        public void setEntityTypes(List<String> entityTypes) {
            this.entityTypes = entityTypes;
        }

        public List<String> getEntityIds() {
            return entityIds;
        }

        public void setEntityIds(List<String> entityIds) {
            this.entityIds = entityIds;
        }

        public List<String> getClassifications() {
            return classifications;
        }

        public void setClassifications(List<String> classifications) {
            this.classifications = classifications;
        }

        public List<String> getAttributes() {
            return attributes;
        }

        public void setAttributes(List<String> attributes) {
            this.attributes = attributes;
        }
    }

}
