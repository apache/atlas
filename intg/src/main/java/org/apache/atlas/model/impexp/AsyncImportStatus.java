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

package org.apache.atlas.model.impexp;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.atlas.model.impexp.AtlasAsyncImportRequest.ImportStatus;

import java.io.Serializable;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AsyncImportStatus implements Serializable {
    private static final long serialVersionUID = 1L;

    private String       importId;
    private ImportStatus status;
    private String       importRequestReceivedTime;
    private String       importRequestUser;

    public AsyncImportStatus() {}

    public AsyncImportStatus(String importId, ImportStatus status, String importRequestReceivedTime, String importRequestUser) {
        this.importId                = importId;
        this.status                  = status;
        this.importRequestReceivedTime = importRequestReceivedTime;
        this.importRequestUser = importRequestUser;
    }

    public String getImportId() {
        return importId;
    }

    public ImportStatus getStatus() {
        return status;
    }

    public String getImportRequestReceivedTime() {
        return importRequestReceivedTime;
    }

    public String getImportRequestUser() {
        return importRequestUser;
    }

    @Override
    public String toString() {
        return "AsyncImportStatus{" +
                "importId='" + importId + '\'' +
                ", status='" + status + '\'' +
                ", importRequestReceivedTime='" + importRequestReceivedTime + '\'' +
                ", importRequestUser='" + importRequestUser + '\'' +
                '}';
    }
}
