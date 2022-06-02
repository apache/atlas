package org.apache.atlas.tasks;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.model.tasks.TaskSearchParams;
import org.apache.atlas.model.tasks.TaskSearchResult;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.DirectIndexQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.atlas.tasks.TaskRegistry.toAtlasTask;

@Component
public class AtlasTaskService implements TaskService {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasTaskService.class);

    private final AtlasGraph graph;

    @Inject
    public AtlasTaskService(AtlasGraph graph) {
        this.graph = graph;
    }

    @Override
    public TaskSearchResult getTasks(TaskSearchParams searchParams) throws AtlasBaseException {
        TaskSearchResult ret = new TaskSearchResult();
        List<AtlasTask> tasks = new ArrayList<>();
        AtlasIndexQuery indexQuery = null;
        DirectIndexQueryResult indexQueryResult;

        try {
            indexQuery = graph.elasticsearchQuery(Constants.VERTEX_INDEX, searchParams);
            indexQueryResult = indexQuery.vertices(searchParams);

            if (indexQueryResult != null) {
                Iterator<AtlasIndexQuery.Result> iterator = indexQueryResult.getIterator();

                while (iterator.hasNext()) {
                    AtlasVertex vertex = iterator.next().getVertex();

                    if (vertex != null) {
                        tasks.add(toAtlasTask(vertex));
                    } else {
                        LOG.warn("Null vertex while fetching tasks");
                    }

                }

                ret.setTasks(tasks);
                ret.setApproximateCount(indexQuery.vertexTotals());
                ret.setAggregations(indexQueryResult.getAggregationMap());
            }
        } catch (AtlasBaseException e) {
            LOG.error("Failed to fetch tasks: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            throw new AtlasBaseException(AtlasErrorCode.RUNTIME_EXCEPTION, e);
        }

        return ret;
    }
}
