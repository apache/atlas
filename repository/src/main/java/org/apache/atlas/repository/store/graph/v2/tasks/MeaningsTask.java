package org.apache.atlas.repository.store.graph.v2.tasks;

import org.apache.atlas.RequestContext;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.exception.EntityNotFoundException;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.repository.store.graph.v2.preprocessor.glossary.TermPreProcessor;
import org.apache.atlas.tasks.AbstractTask;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.atlas.model.tasks.AtlasTask.Status.COMPLETE;
import static org.apache.atlas.model.tasks.AtlasTask.Status.FAILED;

public abstract class MeaningsTask extends AbstractTask {
    private static final Logger LOG = LoggerFactory.getLogger(MeaningsTask.class);
    protected static final String PARAM_ENTITY_GUID = "entityGuid";
    protected static final String PARAM_ENTITY_QUALIFIED_NAME = "entityQname";
    protected static final String PARAM_TERM_NAME = "termName";
    protected final EntityDiscoveryService entityDiscovery;
    protected final EntityGraphMapper entityGraphMapper;
    protected final TermPreProcessor preprocessor;


    public MeaningsTask(AtlasTask task, EntityDiscoveryService entityDiscovery, EntityGraphMapper entityGraphMapper,TermPreProcessor preprocessor) {
        super(task);
        this.entityDiscovery = entityDiscovery;
        this.entityGraphMapper = entityGraphMapper;
        this.preprocessor = preprocessor;
    }

    @Override
    public AtlasTask.Status perform() throws Exception {
        Map<String, Object> params;
        params = getTaskDef().getParameters();
        if (!MapUtils.isEmpty(params)) {
            String userName = getTaskDef().getCreatedBy();

            if (StringUtils.isEmpty(userName)) {
                LOG.warn("Task: {}: Unable to process task as user name is empty!", getTaskGuid());

                return FAILED;
            }

            RequestContext.get().setUser(userName, null);
            try {
                run(params);

                setStatus(COMPLETE);
            } catch (Exception e) {
                LOG.error("Task: {}: Error performing task!", getTaskGuid(), e);

                setStatus(FAILED);

                throw e;
            }
            return getStatus();
        } else {
            LOG.warn("Task: {}: Unable to process task: Parameters is not readable!", getTaskGuid());

            return FAILED;
        }
    }

    public static Map<String, Object> toParameters(String updateTerm, String termQName, String termGuid) {
        return new HashMap<String, Object>() {{
            put(PARAM_ENTITY_GUID, termGuid);
            put(PARAM_ENTITY_QUALIFIED_NAME, termQName);
            put(PARAM_TERM_NAME, updateTerm);
        }};
    }

    protected void setStatus(AtlasTask.Status status) {
        super.setStatus(status);
        try {
            entityGraphMapper.removePendingTaskFromEntity((String) getTaskDef().getParameters().get(PARAM_ENTITY_GUID), getTaskGuid());
        } catch (EntityNotFoundException  e) {
            LOG.error("Error updating associated element for: {}", getTaskGuid(), e);
        }

    }

    protected abstract void run(Map<String, Object> parameters) throws AtlasBaseException;
}
