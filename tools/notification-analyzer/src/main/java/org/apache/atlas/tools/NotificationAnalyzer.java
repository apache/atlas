/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.tools;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.model.notification.HookNotification.EntityCreateRequestV2;
import org.apache.atlas.model.notification.HookNotification.EntityDeleteRequestV2;
import org.apache.atlas.model.notification.HookNotification.EntityPartialUpdateRequestV2;
import org.apache.atlas.model.notification.HookNotification.EntityUpdateRequestV2;
import org.apache.atlas.notification.AtlasNotificationMessageDeserializer;
import org.apache.atlas.notification.NotificationInterface.NotificationType;
import org.apache.atlas.utils.AtlasJson;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.atlas.v1.model.notification.HookNotificationV1.EntityCreateRequest;
import org.apache.atlas.v1.model.notification.HookNotificationV1.EntityDeleteRequest;
import org.apache.atlas.v1.model.notification.HookNotificationV1.EntityPartialUpdateRequest;
import org.apache.atlas.v1.model.notification.HookNotificationV1.EntityUpdateRequest;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IntSummaryStatistics;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

public class NotificationAnalyzer {
    private static final Logger LOG = LoggerFactory.getLogger(NotificationAnalyzer.class);

    private final String                                  msgFile;
    private final String                                  outputFile;
    private final AtlasNotificationMessageDeserializer    deserializer;
    private final Map<String, AtomicInteger>              notificationCountByType = new HashMap<>();
    private final AtomicInteger                           entityCount             = new AtomicInteger();
    private final Map<String, AtomicInteger>              entityCountByType       = new HashMap<>();
    private final Map<String, AtomicInteger>              entityOperCount         = new HashMap<>();
    private final Map<String, Map<String, AtomicInteger>> entityOperByTypeCount   = new HashMap<>();
    private final Set<String>                             knownEntities           = new HashSet<>();
    private final IntSummaryStatistics                    notificationStats       = new IntSummaryStatistics();
    private final IntSummaryStatistics                    splitNotificationStats  = new IntSummaryStatistics();

    public static void main(String[] args) {
        CommandLineParser parser  = new BasicParser();
        Options           options = new Options();

        options.addOption("m", "message-file", true, "Messages file");
        options.addOption("o", "output-file", true, "Output file");

        try {
            CommandLine cmd     = parser.parse(options, args);
            String      msgFile = cmd.getOptionValue("m");
            String      outFile = cmd.getOptionValue("o");

            if (msgFile == null || msgFile.isEmpty()) {
                msgFile = "ATLAS_HOOK.json";
            }

            NotificationAnalyzer analyzer = new NotificationAnalyzer(msgFile, outFile, NotificationType.HOOK);

            analyzer.analyze();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public NotificationAnalyzer(String msgFile, String outputFile, NotificationType notificationType) {
        this.msgFile      = msgFile;
        this.outputFile   = outputFile;
        this.deserializer = notificationType.getDeserializer();
    }

    public void analyze() throws Exception {
        long startTimeMs = System.currentTimeMillis();

        try (BufferedReader reader = getInputReader(); PrintWriter writer = getOutputWriter()) {
            int msgCount         = 0;
            int notificationSize = 0;

            for (String msg = reader.readLine(); msg != null; msg = reader.readLine()) {
                msgCount++;
                notificationSize += msg.length();

                HookNotification notification = (HookNotification) deserializer.deserialize(msg);

                if (notification == null) { // split notification, continue
                    continue;
                }

                notificationStats.accept(notificationSize);

                if (notificationSize > msg.length()) {
                    splitNotificationStats.accept(notificationSize);
                }

                notificationSize = 0;

                notificationCountByType.computeIfAbsent(notification.getType().name(), e -> new AtomicInteger()).incrementAndGet();

                switch (notification.getType()) {
                    case ENTITY_CREATE:
                        handleEntityCreate((EntityCreateRequest) notification);
                        break;
                    case ENTITY_PARTIAL_UPDATE:
                        handleEntityPartialUpdate((EntityPartialUpdateRequest) notification);
                        break;
                    case ENTITY_FULL_UPDATE:
                        handleEntityUpdate((EntityUpdateRequest) notification);
                        break;
                    case ENTITY_DELETE:
                        handleEntityDelete((EntityDeleteRequest) notification);
                        break;
                    case ENTITY_CREATE_V2:
                        handleEntityCreateV2((EntityCreateRequestV2) notification);
                        break;
                    case ENTITY_PARTIAL_UPDATE_V2:
                        handleEntityPartialUpdateV2((EntityPartialUpdateRequestV2) notification);
                        break;
                    case ENTITY_FULL_UPDATE_V2:
                        handleEntityUpdateV2((EntityUpdateRequestV2) notification);
                        break;
                    case ENTITY_DELETE_V2:
                        handleEntityDeleteV2((EntityDeleteRequestV2) notification);
                        break;
                }

                if ((notificationStats.getCount() % 1000) == 0) {
                    LOG.info("PROGRESS #{}: analyzed {} notifications, {} messages", (msgCount / 1000), notificationStats.getCount(), msgCount);
                    writer.printf("PROGRESS #%1$d: analyzed %2$d notifications, %3$d messages%n", (msgCount / 1000), notificationStats.getCount(), msgCount);
                    writer.flush();
                }
            }

            long timeTakenSeconds = (System.currentTimeMillis() - startTimeMs) / 1000;

            LOG.info("Completed analyzing {}. Time taken: {} seconds", msgFile, timeTakenSeconds);
            writer.printf("Completed analyzing %1$s. Time taken: %2$d seconds%n", msgFile, timeTakenSeconds);
            writer.flush();

            Map<String, Object> results = new LinkedHashMap<>();

            results.put("messages", msgCount);
            results.put("notifications", notificationStats.getCount());
            results.put("notificationLengthAvg", (int) notificationStats.getAverage());
            results.put("notificationLengthMax", notificationStats.getMax());
            results.put("splitNotifications", splitNotificationStats.getCount());
            results.put("splitNotificationLengthAvg", (int) splitNotificationStats.getAverage());
            results.put("splitNotificationLengthMax", splitNotificationStats.getMax());
            results.put("entities", entityCount);
            results.put("notificationEntities", entityCountByType.values().stream().mapToInt(AtomicInteger::get).sum());
            results.put("notificationByType", notificationCountByType);
            results.put("notificationEntityByType", entityCountByType);
            results.put("entityOperations", entityOperCount);
            results.put("entityOperationsByType", entityOperByTypeCount);

            String msg = AtlasJson.toJson(results);

            LOG.info(msg);
            writer.println(msg);
            writer.flush();
        }
    }

    private void handleEntityCreate(EntityCreateRequest request) {
        if (request.getEntities() != null) {
            for (Referenceable entity : request.getEntities()) {
                recordEntity(entity);
            }
        }
    }

    private void handleEntityUpdate(EntityUpdateRequest request) {
        if (request.getEntities() != null) {
            for (Referenceable entity : request.getEntities()) {
                recordEntity(entity);
            }
        }
    }

    private void handleEntityPartialUpdate(EntityPartialUpdateRequest request) {
        recordEntityOperation(request.getTypeName(), "PARTIAL_UPDATE");
    }

    private void handleEntityDelete(EntityDeleteRequest request) {
        knownEntities.remove(getEntityKey(request));

        recordEntityOperation(request.getTypeName(), "DELETE");
    }

    private void handleEntityCreateV2(EntityCreateRequestV2 request) {
        if (request.getEntities() != null) {
            if (request.getEntities().getEntities() != null) {
                for (AtlasEntity entity : request.getEntities().getEntities()) {
                    recordEntity(entity);
                }
            }

            if (request.getEntities().getReferredEntities() != null) {
                for (AtlasEntity entity : request.getEntities().getReferredEntities().values()) {
                    recordEntity(entity);
                }
            }
        }
    }

    private void handleEntityUpdateV2(EntityUpdateRequestV2 request) {
        if (request.getEntities() != null) {
            if (request.getEntities().getEntities() != null) {
                for (AtlasEntity entity : request.getEntities().getEntities()) {
                    recordEntity(entity);
                }
            }

            if (request.getEntities().getReferredEntities() != null) {
                for (AtlasEntity entity : request.getEntities().getReferredEntities().values()) {
                    recordEntity(entity);
                }
            }
        }
    }

    private void handleEntityPartialUpdateV2(EntityPartialUpdateRequestV2 request) {
        if (request.getEntity() != null && request.getEntity().getEntity() != null) {
            AtlasEntity entity = request.getEntity().getEntity();

            recordEntityOperation(entity.getTypeName(), "PARTIAL_UPDATE");
        }
    }

    private void handleEntityDeleteV2(EntityDeleteRequestV2 request) {
        if (request.getEntities() != null) {
            for (AtlasObjectId objId : request.getEntities()) {
                knownEntities.remove(getEntityKey(objId));

                recordEntityOperation(objId.getTypeName(), "DELETE");
            }
        }
    }

    private void recordEntity(AtlasEntity entity) {
        final String operation;

        if (knownEntities.add(getEntityKey(entity))) {
            operation = "CREATE";
        } else {
            operation = "UPDATE";
        }

        recordEntityOperation(entity.getTypeName(), operation);
    }

    private void recordEntity(Referenceable entity) {
        final String operation;

        if (knownEntities.add(getEntityKey(entity))) {
            operation = "CREATE";
        } else {
            operation = "UPDATE";
        }

        recordEntityOperation(entity.getTypeName(), operation);
    }

    private void recordEntityOperation(String entityTypeName, String operation) {
        if (operation.equals("CREATE")) {
            entityCount.incrementAndGet();
        }

        entityCountByType.computeIfAbsent(entityTypeName, c -> new AtomicInteger()).incrementAndGet();
        entityOperCount.computeIfAbsent(operation, c -> new AtomicInteger()).incrementAndGet();
        entityOperByTypeCount.computeIfAbsent(operation, c -> new TreeMap<>()).computeIfAbsent(entityTypeName, c -> new AtomicInteger()).incrementAndGet();
    }

    private String getEntityKey(AtlasEntity entity) {
        return entity.getTypeName() + ":" + getUniqueKey(entity.getAttributes());
    }

    private String getEntityKey(AtlasObjectId objectId) {
        return objectId.getTypeName() + ":" + getUniqueKey(objectId.getUniqueAttributes());
    }

    private String getEntityKey(Referenceable entity) {
        return entity.getTypeName() + ":" + getUniqueKey(entity.getValues());
    }

    private String getEntityKey(EntityDeleteRequest request) {
        return request.getTypeName() + ":" + request.getAttributeValue();
    }

    private Object getUniqueKey(Map<String, Object> attributes) {
        Object ret = attributes.get("qualifiedName");

        return ret == null ? attributes.get("name") : ret;
    }

    private BufferedReader getInputReader() throws IOException {
        return new BufferedReader(new FileReader(msgFile));
    }

    private PrintWriter getOutputWriter() throws IOException {
        return (outputFile == null || outputFile.isEmpty()) ? new PrintWriter(System.out) : new PrintWriter(new FileWriter(outputFile, true));
    }
}
