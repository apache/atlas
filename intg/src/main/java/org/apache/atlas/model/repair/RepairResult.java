package org.apache.atlas.model.repair;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@XmlRootElement
public class RepairResult implements Serializable {
    private static final long serialVersionUID = 1L;
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