package org.apache.atlas.repository.graphdb.janus;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import java.util.*;

import static org.apache.atlas.repository.Constants.INDEX_PREFIX;
import static org.apache.atlas.repository.Constants.VERTEX_INDEX;

public class AtlasNestedRelationshipsESQueryBuilder {

    private static final String INDEX_NAME = INDEX_PREFIX + VERTEX_INDEX;
    private static final int RETRY_ON_CONFLICT = 5;

    public static UpdateRequest getQueryForAppendingNestedRelationships(String docId, Map<String, Object> paramsMap) {
        return new UpdateRequest().index(INDEX_NAME)
                .id(docId)
                .retryOnConflict(RETRY_ON_CONFLICT)
                .script(new Script(
                        ScriptType.INLINE, "painless",
                        "if(!ctx._source.containsKey('relationships')) \n" +
                                    "{ctx._source['relationships']=[]}\n" +
                                "int size = ctx._source['relationships'].size();\n" +
                                "ctx._source.relationships.addAll(params.relationships);\n" +
                                "Map i = null;\n" +
                                "Set set = new HashSet();\n" +
                                "for(ListIterator it = ctx._source['relationships'].listIterator(); it.hasNext();){\n" +
                                " i = it.next();\n" +
                                " it.remove();\n" +
                                " set.add(i);\n" +
                                "}\n" +
                                "ctx._source['relationships'].addAll(set);",
                        paramsMap));
    }

    public static UpdateRequest getRelationshipDeletionQuery(String docId, Map<String, Object> params) {
        return new UpdateRequest().index(INDEX_NAME).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).id(docId)
                .script(new Script(
                        ScriptType.INLINE, "painless",
                        "if(ctx._source.containsKey('relationships')){ctx._source.relationships.removeIf(r -> r.relationshipGuid == params.relationshipGuid);}",
                        params
                ));
    }

}