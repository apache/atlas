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
public class RepairEntry implements Serializable {

    private static final long serialVersionUID = 1L;
    private String qualifiedName;
    private String typeName;
    private Long vertexId;
    private String message;

    public RepairEntry(String qualifiedName, String typeName, Long vertexId, String message) {
        this.qualifiedName = qualifiedName;
        this.typeName = typeName;
        this.vertexId = vertexId;
        this.message = message;
    }

    public String getQualifiedName() { return qualifiedName; }
    public String getTypeName() { return typeName; }
    public Long getVertexId() { return vertexId; }
    public String getMessage() { return message; }
}