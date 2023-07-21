package org.apache.atlas.repository.store.graph.v2.tasks;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStoreV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.repository.store.graph.v2.preprocessor.glossary.TermPreProcessor;

import java.util.Map;

public class MeaningsTasks {
    public static class Update extends MeaningsTask {
        public Update(AtlasTask task,  EntityGraphMapper entityGraphMapper, AtlasGraph graph,
                      TermPreProcessor preprocessor) {
            super(task,entityGraphMapper, graph, preprocessor,null);
        }

        @Override
        protected void run(Map<String, Object> parameters) throws AtlasBaseException {
            String termGuid         = (String) parameters.get(PARAM_ENTITY_GUID);
            String termQName        = (String) parameters.get(PARAM_ENTITY_QUALIFIED_NAME);
            String updatedTermQName = (String) parameters.get(PARAM_ENTITY_UPDATED_QUALIFIED_NAME);
            String currentTermName  = (String) parameters.get(PARAM_CURRENT_TERM_NAME);
            String updatedTermName  = (String) parameters.get(PARAM_UPDATED_TERM_NAME);

            preprocessor.updateMeaningsAttributesInEntitiesOnTermUpdate(currentTermName, updatedTermName, termQName, updatedTermQName, termGuid);

        }
    }

    public static class Delete extends MeaningsTask {
        public Delete(AtlasTask task, EntityGraphMapper entityGraphMapper,AtlasGraph graph,
                    AtlasEntityStoreV2 entityStoreV2) {
            super(task,entityGraphMapper, graph, null,entityStoreV2);
        }

        @Override
        protected void run(Map<String, Object> parameters) throws AtlasBaseException {
            String termGuid     = (String) parameters.get(PARAM_ENTITY_GUID);
            String termQName    = (String) parameters.get(PARAM_ENTITY_QUALIFIED_NAME);
            String termName     = (String) parameters.get(PARAM_CURRENT_TERM_NAME);


            entityStoreV2.updateMeaningsNamesInEntitiesOnTermDelete(termName, termQName, termGuid);

        }
    }
}
