package org.apache.atlas.model.repair;


public class RepairResult {
    private boolean repaired;
    private Long repairedVertexId;
    private String message;

    public RepairResult(boolean repaired, Long repairedVertexId, String message) {
        this.repaired = repaired;
        this.repairedVertexId = repairedVertexId;
        this.message = message;
    }

    public boolean isRepaired() {
        return repaired;
    }

    public Long getRepairedVertexId() {
        return repairedVertexId;
    }

    public String getMessage() {
        return message;
    }
}