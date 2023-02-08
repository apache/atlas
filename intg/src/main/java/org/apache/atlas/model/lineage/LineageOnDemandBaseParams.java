package org.apache.atlas.model.lineage;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.atlas.AtlasConfiguration;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class LineageOnDemandBaseParams {
    private int              inputRelationsLimit;
    private int              outputRelationsLimit;

    public static final int LINEAGE_ON_DEMAND_DEFAULT_NODE_COUNT = AtlasConfiguration.LINEAGE_ON_DEMAND_DEFAULT_NODE_COUNT.getInt();

    public LineageOnDemandBaseParams() {
        this.inputRelationsLimit = LINEAGE_ON_DEMAND_DEFAULT_NODE_COUNT;
        this.outputRelationsLimit = LINEAGE_ON_DEMAND_DEFAULT_NODE_COUNT;
    }

    public LineageOnDemandBaseParams(int inputRelationsLimit, int outputRelationsLimit) {
        this.inputRelationsLimit = inputRelationsLimit;
        this.outputRelationsLimit = outputRelationsLimit;
    }

    public int getInputRelationsLimit() {
        return inputRelationsLimit;
    }

    public void setInputRelationsLimit(int inputRelationsLimit) {
        this.inputRelationsLimit = inputRelationsLimit;
    }

    public int getOutputRelationsLimit() {
        return outputRelationsLimit;
    }

    public void setOutputRelationsLimit(int outputRelationsLimit) {
        this.outputRelationsLimit = outputRelationsLimit;
    }
}