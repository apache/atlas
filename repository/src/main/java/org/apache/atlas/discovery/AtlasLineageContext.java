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
package org.apache.atlas.discovery;

import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.lineage.AtlasLineageInfo;
import org.apache.atlas.model.lineage.AtlasLineageRequest;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static org.apache.atlas.model.lineage.AtlasLineageInfo.LineageDirection.BOTH;

public class AtlasLineageContext {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasLineageContext.class);

    private int depth;
    private int offset;
    private int limit;
    private String guid;
    private boolean hideProcess;
    private boolean allowDeletedProcess;
    private boolean calculateRemainingVertexCounts;
    private AtlasLineageInfo.LineageDirection direction = BOTH;

    private boolean isDataset;
    private boolean isProcess;
    private boolean isProduct;

    private Set<String> attributes;
    private Set<String> ignoredProcesses;
    private Predicate predicate;

    private AtlasVertex startDatasetVertex = null;

    public AtlasLineageContext(AtlasLineageRequest lineageRequest, AtlasTypeRegistry typeRegistry) {
        this.guid = lineageRequest.getGuid();
        this.limit = lineageRequest.getLimit();
        this.depth = lineageRequest.getDepth();
        this.direction = lineageRequest.getDirection();
        this.hideProcess = lineageRequest.isHideProcess();
        this.allowDeletedProcess = lineageRequest.isAllowDeletedProcess();
        this.attributes = lineageRequest.getAttributes();
        this.ignoredProcesses = lineageRequest.getIgnoredProcesses();
        this.offset = lineageRequest.getOffset();
        this.calculateRemainingVertexCounts = lineageRequest.getCalculateRemainingVertexCounts();

        predicate = constructInMemoryPredicate(typeRegistry, lineageRequest.getEntityFilters());
    }

    @VisibleForTesting
    AtlasLineageContext() {
    }

    public int getDepth() {
        return depth;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public String getGuid() {
        return guid;
    }

    public void setGuid(String guid) {
        this.guid = guid;
    }

    public boolean isDataset() {
        return isDataset;
    }

    public void setDataset(boolean dataset) {
        isDataset = dataset;
    }

    public boolean isProcess() {
        return isProcess;
    }

    public void setProcess(boolean process) {
        isProcess = process;
    }

    public boolean isProduct() {
        return isProduct;
    }

    public void setProduct(boolean product) {
        isProduct = product;
    }

    public AtlasLineageInfo.LineageDirection getDirection() {
        return direction;
    }

    public void setDirection(AtlasLineageInfo.LineageDirection direction) {
        this.direction = direction;
    }

    public boolean isHideProcess() {
        return hideProcess;
    }

    public void setHideProcess(boolean hideProcess) {
        this.hideProcess = hideProcess;
    }

    public Set<String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Set<String> attributes) {
        this.attributes = attributes;
    }

    public Set<String> getIgnoredProcesses() {
        return ignoredProcesses;
    }

    public void setIgnoredProcesses(Set<String> ignoredProcesses) {
        this.ignoredProcesses = ignoredProcesses;
    }

    public AtlasVertex getStartDatasetVertex() {
        return startDatasetVertex;
    }

    public void setStartDatasetVertex(AtlasVertex startDatasetVertex) {
        this.startDatasetVertex = startDatasetVertex;
    }

    public boolean isAllowDeletedProcess() {
        return allowDeletedProcess;
    }

    public void setAllowDeletedProcess(boolean allowDeletedProcess) {
        this.allowDeletedProcess = allowDeletedProcess;
    }

    public int getOffset() {
        return offset;
    }

    protected Predicate constructInMemoryPredicate(AtlasTypeRegistry typeRegistry, SearchParameters.FilterCriteria filterCriteria) {
        LineageSearchProcessor lineageSearchProcessor = new LineageSearchProcessor();
        return lineageSearchProcessor.constructInMemoryPredicate(typeRegistry, filterCriteria);
    }

    protected boolean evaluate(AtlasVertex vertex) {
        if (predicate != null) {
            return predicate.evaluate(vertex);
        }
        return true;
    }

    public boolean shouldApplyPagination() {
        return offset > -1;
    }

    public boolean isCalculateRemainingVertexCounts() {
        return calculateRemainingVertexCounts;
    }

    @Override
    public String toString() {
        return "LineageRequestContext{" +
                "depth=" + depth +
                ", guid='" + guid + '\'' +
                ", isDataset=" + isDataset +
                ", isProcess=" + isProcess +
                ", allowDeletedProcess=" + allowDeletedProcess +
                ", direction=" + direction +
                ", attributes=" + attributes +
                ", ignoredProcesses=" + ignoredProcesses +
                '}';
    }
}
