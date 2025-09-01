package org.apache.atlas.model.repair;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.xml.bind.annotation.XmlRootElement;


import java.util.ArrayList;
import java.util.List;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;


@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@XmlRootElement
public class BatchRepairResult {
    private static final long serialVersionUID = 1L;
    private List<RepairEntry> successful = new ArrayList<>();
    private List<RepairEntry> failed = new ArrayList<>();
    private List<RepairEntry> skipped = new ArrayList<>();
    private long startTime;
    private long endTime;
    private int totalProcessed;

    public BatchRepairResult() {
        this.startTime = System.currentTimeMillis();
    }

    public void addSuccess(RepairRequest request, RepairResult result) {
        successful.add(new RepairEntry(
                request.getQualifiedName(),
                request.getTypeName(),
                result.getRepairedVertexId(),
                result.getMessage()
        ));
    }

    public void addFailure(RepairRequest request, String error) {
        failed.add(new RepairEntry(
                request.getQualifiedName(),
                request.getTypeName(),
                null,
                error
        ));
    }

    public void addSkipped(RepairRequest request, String reason) {
        skipped.add(new RepairEntry(
                request.getQualifiedName(),
                request.getTypeName(),
                null,
                reason
        ));
    }

    public void finalize() {
        this.endTime = System.currentTimeMillis();
        this.totalProcessed = successful.size() + failed.size() + skipped.size();
    }

    // Getters and Setters
    public int getSuccessCount() {
        return successful.size();
    }

    public int getFailureCount() {
        return failed.size();
    }

    public int getSkippedCount() {
        return skipped.size();
    }

    public List<RepairEntry> getSuccessful() {
        return successful;
    }

    public void setSuccessful(List<RepairEntry> successful) {
        this.successful = successful;
    }

    public List<RepairEntry> getFailed() {
        return failed;
    }

    public void setFailed(List<RepairEntry> failed) {
        this.failed = failed;
    }

    public List<RepairEntry> getSkipped() {
        return skipped;
    }

    public void setSkipped(List<RepairEntry> skipped) {
        this.skipped = skipped;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public int getTotalProcessed() {
        return totalProcessed;
    }

    public void setTotalProcessed(int totalProcessed) {
        this.totalProcessed = totalProcessed;
    }

    public long getExecutionTimeMs() {
        return endTime - startTime;
    }

    public double getSuccessRate() {
        if (totalProcessed == 0) return 0.0;
        return (double) successful.size() / totalProcessed * 100;
    }

    @Override
    public String toString() {
        return "BatchRepairResult{" +
                "successCount=" + getSuccessCount() +
                ", failureCount=" + getFailureCount() +
                ", skippedCount=" + getSkippedCount() +
                ", totalProcessed=" + totalProcessed +
                ", executionTimeMs=" + getExecutionTimeMs() +
                ", successRate=" + String.format("%.2f", getSuccessRate()) + "%" +
                '}';
    }
}