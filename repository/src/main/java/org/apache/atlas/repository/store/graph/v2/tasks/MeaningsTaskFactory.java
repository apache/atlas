package org.apache.atlas.repository.store.graph.v2.tasks;

import org.apache.atlas.model.tasks.AtlasTask;
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

    public static final String MEANINGS_TEXT_UPDATE = "MEANINGS_TEXT_UPDATE";
    public static final String MEANINGS_TEXT_SOFT_DELETE = "MEANINGS_TEXT_SOFT_DELETE";
    public static final String MEANINGS_TEXT_HARD_DELETE = "MEANINGS_TEXT_HARD_DELETE";

    private static final List<String> supportedTypes = new ArrayList<String>() {{
        add(MEANINGS_TEXT_UPDATE);
        add(MEANINGS_TEXT_SOFT_DELETE);
        add(MEANINGS_TEXT_HARD_DELETE);
    }};


    protected final EntityGraphMapper entityGraphMapper;
    protected final TermPreProcessor preprocessor;
    protected  final AtlasEntityStoreV2 entityStoreV2;

    @Inject
    public MeaningsTaskFactory(EntityGraphMapper entityGraphMapper,
                               TermPreProcessor preprocessor,AtlasEntityStoreV2 entityStoreV2) {
        this.entityGraphMapper = entityGraphMapper;
        this.preprocessor = preprocessor;
        this.entityStoreV2 = entityStoreV2;
    }

    @Override
    public org.apache.atlas.tasks.AbstractTask create(AtlasTask atlasTask) {
        String taskType = atlasTask.getType();
        String taskGuid = atlasTask.getGuid();
        switch (taskType) {
            case MEANINGS_TEXT_UPDATE:
                return new MeaningsTasks.Update(atlasTask,entityGraphMapper, preprocessor);
            case MEANINGS_TEXT_SOFT_DELETE:
                return  new MeaningsTasks.Delete(atlasTask,entityGraphMapper,entityStoreV2);
            case MEANINGS_TEXT_HARD_DELETE:
                return new MeaningsTasks.Delete(atlasTask,entityGraphMapper,entityStoreV2);
        }
        LOG.warn("Type: {} - {} not found!. The task will be ignored.", taskType, taskGuid);
        return null;
    }


    @Override
    public List<String> getSupportedTypes() {
        return supportedTypes;
    }
}
