package org.apache.atlas.discovery;

import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.lineage.AtlasLineageInfo;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.testng.Assert.*;

public class EntityLineageServiceTest {


//    public class DirectionIsInputCases {

    @Test
    public void when_it_is_input_and_there_are_more_vertices_it_should_return_false() {
        EntityLineageService entityLineageService = new EntityLineageService();
        AtlasLineageInfo lineageInfo = new AtlasLineageInfo();
        lineageInfo.setGuidEntityMap(new HashMap<>());
        AtlasLineageContext context = new AtlasLineageContext();
        context.setLimit(3);
        context.setDirection(AtlasLineageInfo.LineageDirection.INPUT);
        List<AtlasEdge> currentVertexEdges = fillEdgedWithNull(6000);
        List<AtlasEdge> processEdges = fillEdgedWithNull(3);
        boolean shouldTerminate = entityLineageService.shouldTerminate(true, lineageInfo, context, currentVertexEdges, 0, 50, processEdges, 1);
        assertFalse(shouldTerminate);
    }

    @Test
    public void when_it_is_input_and_there_are_not_any_vertices_it_should_return_true() {
        EntityLineageService entityLineageService = new EntityLineageService();
        AtlasLineageInfo lineageInfo = createFullLineageInfo();
        AtlasLineageContext context = new AtlasLineageContext();
        context.setLimit(3);
        context.setDirection(AtlasLineageInfo.LineageDirection.INPUT);
        List<AtlasEdge> currentVertexEdges = fillEdgedWithNull(6000);
        List<AtlasEdge> processEdges = fillEdgedWithNull(3);
        boolean shouldTerminate = entityLineageService.shouldTerminate(true, lineageInfo, context, currentVertexEdges, 0, 50, processEdges, 1);
        assertTrue(shouldTerminate);
        assertTrue(lineageInfo.getHasMoreUpstreamVertices());
    }

    @Test
    public void when_it_should_terminate_and_there_are_no_more_vertices_has_more_upstream_value_should_be_null() {
        EntityLineageService entityLineageService = new EntityLineageService();
        AtlasLineageInfo lineageInfo = createFullLineageInfo();
        AtlasLineageContext context = new AtlasLineageContext();
        context.setLimit(3);
        context.setDirection(AtlasLineageInfo.LineageDirection.INPUT);
        List<AtlasEdge> currentVertexEdges = fillEdgedWithNull(6000);
        List<AtlasEdge> processEdges = fillEdgedWithNull(3);
        boolean shouldTerminate = entityLineageService.shouldTerminate(true, lineageInfo, context, currentVertexEdges, 0, 5999, processEdges, 2);
        assertTrue(shouldTerminate);
        assertFalse(lineageInfo.getHasMoreUpstreamVertices());
        assertNull(lineageInfo.getHasMoreDownstreamVertices());
    }
//    }

    private static AtlasLineageInfo createFullLineageInfo() {
        AtlasLineageInfo lineageInfo = new AtlasLineageInfo();
        HashMap<String, AtlasEntityHeader> guidEntityMap = new HashMap<>();
        guidEntityMap.put("key1", new AtlasEntityHeader("Process"));
        guidEntityMap.put("key2", new AtlasEntityHeader("Table"));
        guidEntityMap.put("key3", new AtlasEntityHeader("Table"));
        guidEntityMap.put("key4", new AtlasEntityHeader("Table"));
        guidEntityMap.put("key5", new AtlasEntityHeader("Table"));
        lineageInfo.setGuidEntityMap(guidEntityMap);
        return lineageInfo;
    }

    private List<AtlasEdge> fillEdgedWithNull(int edgeCount) {
        List<AtlasEdge> edges = new ArrayList<>();

        for (int i = 0; i < edgeCount; i++) {
            edges.add(null);
        }
        return edges;
    }

}
