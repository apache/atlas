package org.apache.atlas.repository.graph;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusGraph;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.T;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

@Component
public class GremlinShellHelper {

    private static final Logger LOG = LoggerFactory.getLogger(GremlinShellHelper.class);

    private AtlasGraph graph;

    public GremlinShellHelper(AtlasGraph graph) {
        this.graph = graph;
    }


    // gremlin query to get top 10 vertices with most edges
    // g.V().order().by(both().count(),desc).limit(10)
    // .project('guid','edgeCount').by('__guid').by(both().count())
    public  Object getTopXSuperVertex(final int limit) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("GremlinShellHelper.getTopXSuperVertex");
        String edgeCountLabel = "__edgeCount";
        try {
            return ((AtlasJanusGraph) graph).getGraph().traversal()
                    .V()
                    .order()
                    .by(both().count().as("edgeCount"), Order.desc)
                    .limit(limit)
                    .project(GUID_PROPERTY_KEY, TYPE_NAME_PROPERTY_KEY, edgeCountLabel)
                    .by(GUID_PROPERTY_KEY)
                    .by(TYPE_NAME_PROPERTY_KEY)
                    .by("edgeCount")
                    .toStream()
                    .map(m -> {
                        Object guid = m.get(GUID_PROPERTY_KEY);
                        Object typeName = m.get(TYPE_NAME_PROPERTY_KEY);
                        Object edgeCountObj = m.get(edgeCountLabel);

                        String guidStr = (guid != null) ? guid.toString() : "";
                        String typeNameStr = (typeName != null) ? typeName.toString() : "";
                        String edgeCountStr = (edgeCountObj != null) ? edgeCountObj.toString() : "";
                        return new HashMap() {{
                            put(GUID_PROPERTY_KEY, guidStr);
                            put(TYPE_NAME_PROPERTY_KEY, typeNameStr);
                            put(edgeCountLabel, edgeCountStr);
                        }};
                    }).collect(Collectors.toList());
        } catch (Exception e) {
            LOG.error("Error while retrieving rich vertices", e);
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
        }
        finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    //gremlin query to retrieve relationship stats of a vertex
    //  g.V().has('__guid', '').bothE().groupCount().by("__typeName")
    public  Object getVertexRelationshipStats(final String guid) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("GremlinShellHelper.getVertexRelationshipStats");
        try {
             return  ((AtlasJanusGraph) graph).getGraph().traversal()
                    .V()
                    .has(GUID_PROPERTY_KEY, guid)
                     .bothE()
                     .groupCount()
                     .by(T.label)
                     .next();
        } catch (Exception e) {
            LOG.error("Error while getting relationship stats ", e);
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
        }
        finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }
}
