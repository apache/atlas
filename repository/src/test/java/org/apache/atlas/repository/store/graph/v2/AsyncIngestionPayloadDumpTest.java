package org.apache.atlas.repository.store.graph.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.store.graph.v2.AsyncIngestionEventType;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.typedef.*;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.*;

/**
 * Generates sample Kafka payloads for every async ingestion event type and writes them to a file.
 *
 * Run:
 *   mvn test -pl repository -am -Dtest=AsyncIngestionPayloadDumpTest -DskipTests=false -Drat.skip=true
 *
 * Output: repository/target/async-ingestion-payloads.json
 */
@Disabled("Utility test for generating sample payloads to a file — run manually when needed")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class AsyncIngestionPayloadDumpTest {

    private static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private static final String OUTPUT_FILE = "target/async-ingestion-payloads.json";

    private final List<Object> allPayloads = new ArrayList<>();

    @AfterAll
    void writePayloadsToFile() throws Exception {
        File outFile = new File(OUTPUT_FILE);
        outFile.getParentFile().mkdirs();
        try (PrintWriter pw = new PrintWriter(new FileWriter(outFile))) {
            pw.println(MAPPER.writeValueAsString(allPayloads));
        }
        System.out.println("\n========================================");
        System.out.println("Payloads written to: " + outFile.getAbsolutePath());
        System.out.println("Total events: " + allPayloads.size());
        System.out.println("========================================\n");
    }

    /**
     * Builds a Kafka event envelope exactly matching what AsyncIngestionProducer produces.
     * requestMetadata only carries traceId + user (no requestUri/requestMethod).
     */
    private ObjectNode buildEnvelope(String eventType, Map<String, Object> opMeta, Object payload) {
        ObjectNode env = MAPPER.createObjectNode();
        env.put("eventId", UUID.randomUUID().toString());
        env.put("eventType", eventType);
        env.put("eventTime", System.currentTimeMillis());

        ObjectNode reqMeta = MAPPER.createObjectNode();
        reqMeta.put("traceId", "trace-" + UUID.randomUUID().toString().substring(0, 8));
        reqMeta.put("user", "admin");
        env.set("requestMetadata", reqMeta);

        env.set("operationMetadata", MAPPER.valueToTree(opMeta));
        env.set("payload", MAPPER.valueToTree(payload));
        return env;
    }

    private void store(ObjectNode envelope) {
        allPayloads.add(envelope);
    }

    // ============================= Entity Events =============================

    @Test @Order(1)
    void payload_BULK_CREATE_OR_UPDATE() {
        AtlasEntity entity1 = new AtlasEntity("Table");
        entity1.setGuid("guid-table-001");
        entity1.setAttribute("qualifiedName", "default/snowflake/db1/schema1/table1");
        entity1.setAttribute("name", "table1");
        entity1.setAttribute("description", "Sample table for async ingestion payload demo");

        AtlasEntity entity2 = new AtlasEntity("Column");
        entity2.setGuid("guid-col-001");
        entity2.setAttribute("qualifiedName", "default/snowflake/db1/schema1/table1/col1");
        entity2.setAttribute("name", "col1");
        entity2.setAttribute("dataType", "VARCHAR");

        AtlasEntitiesWithExtInfo payload = new AtlasEntitiesWithExtInfo();
        payload.addEntity(entity1);
        payload.addEntity(entity2);

        Map<String, Object> opMeta = new LinkedHashMap<>();
        opMeta.put("replaceClassifications", false);
        opMeta.put("replaceTags", false);
        opMeta.put("appendTags", false);
        opMeta.put("replaceBusinessAttributes", false);
        opMeta.put("overwriteBusinessAttributes", false);
        opMeta.put("skipProcessEdgeRestoration", false);

        store(buildEnvelope(AsyncIngestionEventType.BULK_CREATE_OR_UPDATE, opMeta, payload));
    }

    @Test @Order(2)
    void payload_SET_CLASSIFICATIONS() {
        AtlasClassification c1 = new AtlasClassification("PII");
        c1.setEntityGuid("guid-table-001");
        c1.setAttribute("level", "HIGH");

        AtlasClassification c2 = new AtlasClassification("Confidential");
        c2.setEntityGuid("guid-col-001");

        store(buildEnvelope(AsyncIngestionEventType.SET_CLASSIFICATIONS, Collections.emptyMap(), List.of(c1, c2)));
    }

    @Test @Order(3)
    void payload_DELETE_BY_GUID() {
        Map<String, Object> opMeta = new LinkedHashMap<>();
        opMeta.put("deleteType", "SOFT");

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("guid", "guid-table-001");

        store(buildEnvelope(AsyncIngestionEventType.DELETE_BY_GUID, opMeta, payload));
    }

    @Test @Order(4)
    void payload_DELETE_BY_GUIDS() {
        Map<String, Object> opMeta = new LinkedHashMap<>();
        opMeta.put("deleteType", "SOFT");

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("guids", List.of("guid-table-001", "guid-col-001", "guid-col-002"));

        store(buildEnvelope(AsyncIngestionEventType.DELETE_BY_GUIDS, opMeta, payload));
    }

    @Test @Order(5)
    void payload_DELETE_BY_UNIQUE_ATTRIBUTE() {
        AtlasObjectId objId = new AtlasObjectId("Table", "qualifiedName",
                "default/snowflake/db1/schema1/table1");

        Map<String, Object> opMeta = new LinkedHashMap<>();
        opMeta.put("deleteType", "SOFT");
        opMeta.put("typeName", "Table");

        store(buildEnvelope(AsyncIngestionEventType.DELETE_BY_UNIQUE_ATTRIBUTE, opMeta, objId));
    }

    @Test @Order(6)
    void payload_BULK_DELETE_BY_UNIQUE_ATTRIBUTES() {
        AtlasObjectId obj1 = new AtlasObjectId("Table", "qualifiedName",
                "default/snowflake/db1/schema1/table1");
        AtlasObjectId obj2 = new AtlasObjectId("Table", "qualifiedName",
                "default/snowflake/db1/schema1/table2");

        Map<String, Object> opMeta = new LinkedHashMap<>();
        opMeta.put("deleteType", "SOFT");

        store(buildEnvelope(AsyncIngestionEventType.BULK_DELETE_BY_UNIQUE_ATTRIBUTES, opMeta, List.of(obj1, obj2)));
    }

    @Test @Order(7)
    void payload_RESTORE_BY_GUIDS() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("guids", List.of("guid-table-001", "guid-col-001"));

        store(buildEnvelope(AsyncIngestionEventType.RESTORE_BY_GUIDS, Collections.emptyMap(), payload));
    }

    // ============================= TypeDef Events =============================

    @Test @Order(8)
    void payload_TYPEDEF_CREATE() {
        AtlasEntityDef entityDef = new AtlasEntityDef("CustomTable");
        entityDef.setDescription("A custom table type");
        entityDef.setSuperTypes(new HashSet<>(List.of("DataSet")));
        entityDef.setAttributeDefs(List.of(
                new AtlasStructDef.AtlasAttributeDef("customField", "string")
        ));

        AtlasClassificationDef classifDef = new AtlasClassificationDef("SensitiveData");
        classifDef.setDescription("Marks data as sensitive");

        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.setEntityDefs(List.of(entityDef));
        typesDef.setClassificationDefs(List.of(classifDef));

        Map<String, Object> opMeta = new LinkedHashMap<>();
        opMeta.put("allowDuplicateDisplayName", false);

        store(buildEnvelope(AsyncIngestionEventType.TYPEDEF_CREATE, opMeta, typesDef));
    }

    @Test @Order(9)
    void payload_TYPEDEF_UPDATE() {
        AtlasEntityDef entityDef = new AtlasEntityDef("CustomTable");
        entityDef.setDescription("Updated custom table type with new attribute");
        entityDef.setSuperTypes(new HashSet<>(List.of("DataSet")));
        entityDef.setAttributeDefs(List.of(
                new AtlasStructDef.AtlasAttributeDef("customField", "string"),
                new AtlasStructDef.AtlasAttributeDef("newField", "int")
        ));

        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.setEntityDefs(List.of(entityDef));

        Map<String, Object> opMeta = new LinkedHashMap<>();
        opMeta.put("allowDuplicateDisplayName", false);
        opMeta.put("patch", true);

        store(buildEnvelope(AsyncIngestionEventType.TYPEDEF_UPDATE, opMeta, typesDef));
    }

    @Test @Order(10)
    void payload_TYPEDEF_DELETE() {
        AtlasEntityDef entityDef = new AtlasEntityDef("CustomTable");
        AtlasClassificationDef classifDef = new AtlasClassificationDef("SensitiveData");

        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.setEntityDefs(List.of(entityDef));
        typesDef.setClassificationDefs(List.of(classifDef));

        store(buildEnvelope(AsyncIngestionEventType.TYPEDEF_DELETE, Collections.emptyMap(), typesDef));
    }

    @Test @Order(11)
    void payload_TYPEDEF_DELETE_BY_NAME() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("typeName", "CustomTable");

        store(buildEnvelope(AsyncIngestionEventType.TYPEDEF_DELETE_BY_NAME, Collections.emptyMap(), payload));
    }
}
