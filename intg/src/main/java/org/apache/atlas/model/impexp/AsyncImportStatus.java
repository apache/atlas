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

public class AsyncImportStatus {
    private String importId;
    private AtlasAsyncImportRequest.ImportStatus status;
    private String importRequestReceivedAt;
    private String importRequestReceivedBy;

    public AsyncImportStatus() {}

    public AsyncImportStatus(String importId, AtlasAsyncImportRequest.ImportStatus status, String importRequestReceivedAt, String importRequestReceivedBy) {
        this.importId = importId;
        this.status = status;
        this.importRequestReceivedAt = importRequestReceivedAt;
        this.importRequestReceivedBy = importRequestReceivedBy;
    }

    public String getImportId() {
        return importId;
    }

    public AtlasAsyncImportRequest.ImportStatus getStatus() {
        return status;
    }

    public String getImportRequestReceivedAt() {
        return importRequestReceivedAt;
    }

    public String getImportRequestReceivedBy() {
        return importRequestReceivedBy;
    }
}
