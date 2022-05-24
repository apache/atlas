package org.apache.atlas.repository.store.graph.v2.tasks;

import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStoreV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.repository.store.graph.v2.preprocessor.glossary.TermPreProcessor;
import org.apache.atlas.tasks.TaskFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

@Component
public class MeaningsTaskFactory implements TaskFactory {
    private static final Logger LOG = LoggerFactory.getLogger(MeaningsTaskFactory.class);

    public static final String UPDATE_ENTITY_MEANINGS_ON_TERM_UPDATE = "UPDATE_ENTITY_MEANINGS_ON_TERM_UPDATE";
    public static final String UPDATE_ENTITY_MEANINGS_ON_TERM_SOFT_DELETE = "UPDATE_ENTITY_MEANINGS_ON_TERM_SOFT_DELETE";
    public static final String UPDATE_ENTITY_MEANINGS_ON_TERM_HARD_DELETE = "UPDATE_ENTITY_MEANINGS_ON_TERM_HARD_DELETE";

    private static final List<String> supportedTypes = new ArrayList<String>() {{
        add(UPDATE_ENTITY_MEANINGS_ON_TERM_UPDATE);
        add(UPDATE_ENTITY_MEANINGS_ON_TERM_SOFT_DELETE);
        add(UPDATE_ENTITY_MEANINGS_ON_TERM_HARD_DELETE);
    }};


    protected final EntityGraphMapper entityGraphMapper;
    protected final TermPreProcessor preprocessor;
    protected  final AtlasEntityStoreV2 entityStoreV2;
    protected final AtlasGraph graph;

    @Inject
    public MeaningsTaskFactory(EntityGraphMapper entityGraphMapper,
                               TermPreProcessor preprocessor, AtlasEntityStoreV2 entityStoreV2, AtlasGraph graph) {
        this.entityGraphMapper = entityGraphMapper;
        this.preprocessor = preprocessor;
        this.entityStoreV2 = entityStoreV2;
        this.graph = graph;
    }

    @Override
    public org.apache.atlas.tasks.AbstractTask create(AtlasTask atlasTask) {
        String taskType = atlasTask.getType();
        String taskGuid = atlasTask.getGuid();
        switch (taskType) {
            case UPDATE_ENTITY_MEANINGS_ON_TERM_UPDATE:
                return new MeaningsTasks.Update(atlasTask, entityGraphMapper, graph, preprocessor);
            case UPDATE_ENTITY_MEANINGS_ON_TERM_SOFT_DELETE:
                return new MeaningsTasks.Delete(atlasTask, entityGraphMapper, graph, entityStoreV2);
            case UPDATE_ENTITY_MEANINGS_ON_TERM_HARD_DELETE:
                return new MeaningsTasks.Delete(atlasTask, entityGraphMapper, graph, entityStoreV2);
        }
        LOG.warn("Type: {} - {} not found!. The task will be ignored.", taskType, taskGuid);
        return null;
    }


    @Override
    public List<String> getSupportedTypes() {
        return supportedTypes;
    }
}
