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
package org.apache.atlas.model.discovery;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * Request object for discovering Purpose entities accessible to a user.
 * Used by the POST /api/meta/purposes/user endpoint.
 */
@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class PurposeUserRequest implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final int DEFAULT_LIMIT = 100;
    public static final int MAX_LIMIT = 500;

    private String username;
    private List<String> groups;
    private Set<String> attributes;
    private int limit = DEFAULT_LIMIT;
    private int offset = 0;

    public PurposeUserRequest() {
    }

    public PurposeUserRequest(String username, List<String> groups) {
        this.username = username;
        this.groups = groups;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public List<String> getGroups() {
        return groups;
    }

    public void setGroups(List<String> groups) {
        this.groups = groups;
    }

    public Set<String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Set<String> attributes) {
        this.attributes = attributes;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    /**
     * Validates the request and throws IllegalArgumentException if invalid.
     */
    public void validate() throws IllegalArgumentException {
        if (StringUtils.isBlank(username)) {
            throw new IllegalArgumentException("username is required");
        }
        if (groups == null) {
            throw new IllegalArgumentException("groups is required (can be empty list)");
        }
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be at least 1");
        }
        if (limit > MAX_LIMIT) {
            throw new IllegalArgumentException("limit cannot exceed " + MAX_LIMIT);
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be non-negative");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PurposeUserRequest that = (PurposeUserRequest) o;
        return limit == that.limit &&
                offset == that.offset &&
                Objects.equals(username, that.username) &&
                Objects.equals(groups, that.groups) &&
                Objects.equals(attributes, that.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(username, groups, attributes, limit, offset);
    }

    @Override
    public String toString() {
        return "PurposeUserRequest{" +
                "username='" + username + '\'' +
                ", groups=" + groups +
                ", attributes=" + attributes +
                ", limit=" + limit +
                ", offset=" + offset +
                '}';
    }
}
