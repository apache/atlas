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
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.atlas.model.tasks.AtlasTask;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AtlasSearchResultDownloadStatus implements Serializable {
    private List<AtlasSearchDownloadRecord> searchDownloadRecords;

    public List<AtlasSearchDownloadRecord> getSearchDownloadRecords() {
        return searchDownloadRecords;
    }

    public void setSearchDownloadRecords(List<AtlasSearchDownloadRecord> searchDownloadRecords) {
        this.searchDownloadRecords = searchDownloadRecords;
    }

    @JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.PROPERTY)
    public static class AtlasSearchDownloadRecord implements Serializable {
        private AtlasTask.Status status;
        private String           fileName;
        private String           createdBy;
        private Date             createdTime;
        private Date             startTime;

        public AtlasSearchDownloadRecord(AtlasTask.Status status, String fileName, String createdBy, Date createdTime, Date startTime) {
            this.status      = status;
            this.fileName    = fileName;
            this.createdBy   = createdBy;
            this.createdTime = createdTime;
            this.startTime   = startTime;
        }

        public AtlasSearchDownloadRecord(AtlasTask.Status status, String fileName, String createdBy, Date createdTime) {
            this(status, fileName, createdBy, createdTime, null);
        }

        public AtlasTask.Status getStatus() {
            return status;
        }

        public void setStatus(AtlasTask.Status status) {
            this.status = status;
        }

        public String getFileName() {
            return fileName;
        }

        public void setFileName(String fileName) {
            this.fileName = fileName;
        }

        public String getCreatedBy() {
            return createdBy;
        }

        public void setCreatedBy(String createdBy) {
            this.createdBy = createdBy;
        }

        public Date getCreatedTime() {
            return createdTime;
        }

        public void setCreatedTime(Date createdTime) {
            this.createdTime = createdTime;
        }

        public Date getStartTime() {
            return startTime;
        }

        public void setStartTime(Date startTime) {
            this.startTime = startTime;
        }

        public StringBuilder toString(StringBuilder sb) {
            if (sb == null) {
                sb = new StringBuilder();
            }

            sb.append("AtlasSearchDownloadRecord{");
            sb.append("status=").append(status);
            sb.append(", fileName=").append(fileName);
            sb.append(", createdBy=").append(createdBy);
            sb.append(", createTime=").append(createdTime);
            sb.append(", startTime=").append(startTime);
            sb.append("}");

            return sb;
        }

        @Override
        public String toString() {
            return toString(new StringBuilder()).toString();
        }
    }
}
