package org.apache.hadoop.metadata;

import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.metadata.services.TitanGraphService;
import org.apache.hadoop.metadata.util.GraphUtils;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.UUID;

/**
 * End to end graph put/get test.
 */
public class TitanGraphServiceIntegrationTest {

    @Test
    public void testTitanGraph() throws Exception {
        TitanGraphService titanGraphService = new TitanGraphService();
        titanGraphService.start();

        try {
            String guid = UUID.randomUUID().toString();

            final TransactionalGraph graph = titanGraphService.getTransactionalGraph();
            System.out.println("graph = " + graph);
            System.out.println("graph.getVertices() = " + graph.getVertices());


            Vertex entityVertex = null;
            try {
                graph.rollback();
                entityVertex = graph.addVertex(null);
                entityVertex.setProperty("guid", guid);
                entityVertex.setProperty("entityName", "entityName");
                entityVertex.setProperty("entityType", "entityType");
            } catch (Exception e) {
                graph.rollback();
                e.printStackTrace();
            } finally {
                graph.commit();
            }

            System.out.println("vertex = " + GraphUtils.vertexString(entityVertex));

            GraphQuery query = graph.query()
                    .has("entityName", "entityName")
                    .has("entityType", "entityType");

            Iterator<Vertex> results = query.vertices().iterator();
            if (results.hasNext()) {
                Vertex vertexFromQuery = results.next();
                System.out.println("vertex = " + GraphUtils.vertexString(vertexFromQuery));
            }
        } finally {
            Thread.sleep(1000);
            titanGraphService.stop();
        }
    }
}
