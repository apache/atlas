/**
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.model.instance;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;
import java.util.Objects;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * Represents a single GUID that failed during a mutation operation, with error code and message.
 */
@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class FailedEntity implements Serializable {
    private static final long serialVersionUID = 1L;

    private String guid;
    private String errorCode;
    private String errorMessage;

    public FailedEntity() {
    }

    public FailedEntity(String guid, String errorCode, String errorMessage) {
        this.guid         = guid;
        this.errorCode    = errorCode;
        this.errorMessage = errorMessage;
    }

    public String getGuid() {
        return guid;
    }

    public void setGuid(String guid) {
        this.guid = guid;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("FailedEntity{");
        sb.append("guid='").append(guid).append('\'');
        sb.append(", errorCode='").append(errorCode).append('\'');
        sb.append(", errorMessage='").append(errorMessage).append('\'');
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FailedEntity that = (FailedEntity) o;
        return Objects.equals(guid, that.guid) &&
               Objects.equals(errorCode, that.errorCode) &&
               Objects.equals(errorMessage, that.errorMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(guid, errorCode, errorMessage);
    }
}
