package org.apache.atlas.discovery;

import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.lineage.LineageOnDemandBaseParams;
import org.apache.atlas.model.lineage.LineageOnDemandConstraints;
import org.apache.atlas.model.lineage.LineageOnDemandRequest;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

import static org.apache.atlas.model.lineage.LineageListRequest.LINEAGE_TYPE_DATASET_PROCESS_LINEAGE;


public class AtlasLineageOnDemandContext {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasLineageContext.class);

    private Map<String, LineageOnDemandConstraints> constraints;
    private Predicate                               vertexPredicate;
    private Predicate                               edgePredicate;
    private Set<String>                             attributes;
    private Set<String>                             relationAttributes;
    private LineageOnDemandBaseParams               defaultParams;


    private String                                  lineageType = LINEAGE_TYPE_DATASET_PROCESS_LINEAGE;

    public AtlasLineageOnDemandContext(LineageOnDemandRequest lineageOnDemandRequest, AtlasTypeRegistry typeRegistry) {
        this.constraints        = lineageOnDemandRequest.getConstraints();
        this.attributes         = lineageOnDemandRequest.getAttributes();
        this.relationAttributes = lineageOnDemandRequest.getRelationAttributes();
        this.defaultParams      = lineageOnDemandRequest.getDefaultParams();
        this.vertexPredicate    = constructInMemoryPredicate(typeRegistry, lineageOnDemandRequest.getEntityTraversalFilters());
        this.edgePredicate      = constructInMemoryPredicate(typeRegistry, lineageOnDemandRequest.getRelationshipTraversalFilters());
        this.lineageType        = lineageOnDemandRequest.getLineageType();
    }

    public Map<String, LineageOnDemandConstraints> getConstraints() {
        return constraints;
    }

    public void setConstraints(Map<String, LineageOnDemandConstraints> constraints) {
        this.constraints = constraints;
    }

    public Predicate getVertexPredicate() {
        return vertexPredicate;
    }

    public void setVertexPredicate(Predicate vertexPredicate) {
        this.vertexPredicate = vertexPredicate;
    }

    public Predicate getEdgePredicate() {
        return edgePredicate;
    }

    public void setEdgePredicate(Predicate edgePredicate) {
        this.edgePredicate = edgePredicate;
    }
    public String getLineageType() {
        return lineageType;
    }

    public void setLineageType(String lineageType) {
        this.lineageType = lineageType;
    }

    public Set<String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Set<String> attributes) {
        this.attributes = attributes;
    }

    public Set<String> getRelationAttributes() {
        return relationAttributes;
    }

    public void setRelationAttributes(Set<String> relationAttributes) {
        this.relationAttributes = relationAttributes;
    }

    public LineageOnDemandBaseParams getDefaultParams() {
        return defaultParams;
    }

    public void setDefaultParams(LineageOnDemandBaseParams defaultParams) {
        this.defaultParams = defaultParams;
    }

    protected Predicate constructInMemoryPredicate(AtlasTypeRegistry typeRegistry, SearchParameters.FilterCriteria filterCriteria) {
        LineageSearchProcessor lineageSearchProcessor = new LineageSearchProcessor();
        return lineageSearchProcessor.constructInMemoryPredicate(typeRegistry, filterCriteria);
    }

    protected boolean evaluate(AtlasVertex vertex) {
        if (vertexPredicate != null) {
            return vertexPredicate.evaluate(vertex);
        }
        return true;
    }

    protected boolean evaluate(AtlasEdge edge) {
        if (edgePredicate != null) {
            return edgePredicate.evaluate(edge);
        }
        return true;
    }
}
