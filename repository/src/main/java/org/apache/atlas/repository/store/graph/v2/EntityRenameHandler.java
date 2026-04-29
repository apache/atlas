/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.type.RenamePropagationTarget;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class EntityRenameHandler {
    private static final Logger LOG      = LoggerFactory.getLogger(EntityRenameHandler.class);

    private final AtlasTypeRegistry          typeRegistry;

    @Inject
    public EntityRenameHandler(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry            = typeRegistry;
    }

    /**
     * Discovers all entities whose qualifiedName is impacted by the given rename and injects
     * them into the mutation context so they are persisted in the same transaction.
     *
     * Uses typedef-time metadata on each {@link AtlasEntityType} for O(1) slot-key lookup where
     * applicable, without deriving template paths from the graph at runtime.
     */
    public void addDependentsToContext(EntityMutationContext context,
                                       AtlasEntityType       sourceEntityType,
                                       AtlasVertex           sourceVertex,
                                       AtlasEntity           sourceEntity) throws AtlasBaseException {
        String renamedTypeName = sourceEntityType.getTypeName();
        String newName         = (String) sourceEntity.getAttribute(AtlasTypeUtil.ATTRIBUTE_NAME);

        LOG.debug("addDependentsToContext(): start — renamedType={}, newName={}", renamedTypeName, newName);

        Map<String, DependentUpdate> dependents   = new LinkedHashMap<>();
        Set<String>                  visitedGuids = new HashSet<>();

        collectDependents(sourceVertex, sourceEntityType, renamedTypeName, newName, visitedGuids, dependents);

        LOG.info("SANKET:DEBUG:HOOK: dependents: {}", dependents);

        LOG.debug("addDependentsToContext(): done — renamedType={}, dependentCount={}", renamedTypeName, dependents.size());

        injectDependentsIntoContext(context, dependents);
        LOG.info("SANKET:DEBUG:HOOK: context.getUpdatedEntities(): {}", context.getUpdatedEntities());
    }

    // ─── context injection ──────────────────────────────────────────────────────

    private void injectDependentsIntoContext(EntityMutationContext context, Map<String, DependentUpdate> dependents) {
        for (DependentUpdate dep : dependents.values()) {
            AtlasEntity stub = new AtlasEntity(dep.getTypeName());

            stub.setGuid(dep.getGuid());
            stub.setAttribute(AtlasTypeUtil.ATTRIBUTE_QUALIFIED_NAME, dep.getNewUniqueAttribute());

            if (MapUtils.isNotEmpty(dep.getMappedAttrs())) {
                for (Map.Entry<String, Object> entry : dep.getMappedAttrs().entrySet()) {
                    stub.setAttribute(entry.getKey(), entry.getValue());
                }
            }

            AtlasEntityType dependentEntityType = typeRegistry.getEntityTypeByName(dep.getTypeName());

            context.addUpdated(dep.getGuid(), stub, dependentEntityType, dep.getVertex());
        }
    }

    // ─── core traversal ─────────────────────────────────────────────────────────

    /**
     * Single recursive traversal over all propagation targets.
     *
     * <p>The override strategy is decided purely by whether the target carries
     * explicit {@code propagateAttributes} mappings:
     *
     * <ul>
     *   <li><b>propagateAttributes present:</b> the slot patched in the dependent's {@code qualifiedName}
     *       template is the {@code target} of the mapping whose {@code source} is {@code "name"}
     *       (see {@link #findQualifiedNameOverrideKey}); {@code newName} is written into that slot.
     *       Extra stub attributes come from {@link #buildMappedAttrs}. The updated dependent becomes
     *       the new rename root for any further cascade.</li>
     *   <li><b>propagateAttributes absent:</b> the override slot is resolved via O(1) lookup
     *       in the dependent type's precomputed {@code renamePropagationTemplateMap} using
     *       {@code renamedTypeName}. The same renamed type and new name are propagated
     *       unchanged through the cascade.</li>
     * </ul>
     */
    private void collectDependents(AtlasVertex sourceVertex, AtlasEntityType sourceEntityType,
                                   String renamedTypeName, String newName,
                                   Set<String> visitedGuids, Map<String, DependentUpdate> out) throws AtlasBaseException {
        for (RenamePropagationTarget propagationTarget : sourceEntityType.getRenamePropagationTargets()) {
            AtlasAttribute relAttr = propagationTarget.getRelAttr();

            if (relAttr == null) {
                continue;
            }

            for (AtlasEdge edge : toList(GraphHelper.getEdgesForLabel(sourceVertex, relAttr.getRelationshipEdgeLabel(), relAttr.getRelationshipEdgeDirection()))) {
                AtlasVertex dependentVertex = getOtherVertex(edge, sourceVertex);

                if (dependentVertex == null) {
                    continue;
                }

                String dependentGuid = AtlasGraphUtilsV2.getIdFromVertex(dependentVertex);

                if (StringUtils.isBlank(dependentGuid) || !visitedGuids.add(dependentGuid)) {
                    continue;
                }

                String          dependentTypeName   = GraphHelper.getTypeName(dependentVertex);
                AtlasEntityType dependentEntityType = typeRegistry.getEntityTypeByName(dependentTypeName);

                if (dependentEntityType == null) {
                    continue;
                }

                String template = getUniqueAttributeTemplate(dependentEntityType);
                String oldQN    = AtlasGraphUtilsV2.getProperty(dependentVertex,
                        dependentEntityType.getVertexPropertyName(AtlasTypeUtil.ATTRIBUTE_QUALIFIED_NAME), String.class);

                LOG.info("SANKET:DEBUG:HOOK: oldQN: {}", oldQN);
                LOG.info("SANKET:DEBUG:HOOK: template: {}", template);
                LOG.info("SANKET:DEBUG:HOOK: dependentTypeName: {}", dependentTypeName);

                if (StringUtils.isBlank(template) || StringUtils.isBlank(oldQN)) {
                    continue;
                }

                String              overrideKey;
                Map<String, Object> mappedAttrs;

                if (CollectionUtils.isNotEmpty(propagationTarget.getPropagateAttributes())) {
                    // QualifiedName slot key comes from the propagateAttributes entry whose source is "name".
                    overrideKey = findQualifiedNameOverrideKey(propagationTarget.getPropagateAttributes());
                    if (overrideKey == null) {
                        overrideKey = AtlasTypeUtil.ATTRIBUTE_NAME;
                    }
                    mappedAttrs = buildMappedAttrs(propagationTarget, newName);
                } else {
                    // No propagateAttributes: O(1) lookup of the slot key from the typedef-time
                    // precomputed renamePropagationTemplateMap on the dependent entity type.
                    overrideKey = dependentEntityType.getRenamePropagationTemplateMap().get(renamedTypeName);

                    if (overrideKey == null) {
                        LOG.debug("collectDependents(): no override key for renamedType={} on dependentType={} — skipping",
                                renamedTypeName, dependentTypeName);
                        continue;
                    }

                    mappedAttrs = new HashMap<>();
                }

                String newQN = recomputeUniqueAttribute(oldQN, template, overrideKey, newName);

                if (StringUtils.isBlank(newQN) || StringUtils.equals(oldQN, newQN)) {
                    continue;
                }

                LOG.debug("collectDependents(): updating {} [{}] {} -> {}", dependentTypeName, dependentGuid, oldQN, newQN);

                out.put(dependentGuid, new DependentUpdate(dependentGuid, dependentTypeName, dependentVertex, newQN, mappedAttrs));

                if (CollectionUtils.isNotEmpty(dependentEntityType.getRenamePropagationTargets())) {
                    if (CollectionUtils.isNotEmpty(propagationTarget.getPropagateAttributes())) {
                        // The updated dependent becomes the new rename root for its own downstream cascade.
                        // Read the segment we patched (same key as overrideKey) for the next level's newName.
                        String dependentNewName = parseUniqueAttribute(newQN, template).get(overrideKey);

                        if (StringUtils.isBlank(dependentNewName)) {
                            dependentNewName = newName;
                        }

                        collectDependents(dependentVertex, dependentEntityType, dependentTypeName, dependentNewName, visitedGuids, out);
                    } else {
                        // Template-slot cascade: propagate the same renamed type and new name unchanged.
                        collectDependents(dependentVertex, dependentEntityType, renamedTypeName, newName, visitedGuids, out);
                    }
                }
            }
        }
    }

    // ─── association attribute mapping ──────────────────────────────────────────

    /**
     * Builds the mapped-attribute payload for an association-linked dependent.
     * Reads {@link RenamePropagationTarget#getPropagateAttributes()} so renamed values can be written
     * to differently named attributes on the dependent stub when the mapping source is {@code name}.
     * Falls back to {@code {name → newSourceEntityName}} when the list is empty.
     */
    private Map<String, Object> buildMappedAttrs(RenamePropagationTarget target, String newSourceEntityName) {
        List<Map<String, String>> propagateAttributes = target.getPropagateAttributes();

        if (CollectionUtils.isEmpty(propagateAttributes)) {
            Map<String, Object> fallback = new HashMap<>();

            fallback.put(AtlasTypeUtil.ATTRIBUTE_NAME, newSourceEntityName);

            return fallback;
        }

        Map<String, Object> mappedAttrs = new HashMap<>();

        for (Map<String, String> mapping : propagateAttributes) {
            String sourceAttr = mapping.get("source");
            String targetAttr = mapping.get("target");

            // Only "name" is available at this point; other source attributes would need the full sourceEntity.
            if (AtlasTypeUtil.ATTRIBUTE_NAME.equals(sourceAttr) && StringUtils.isNotBlank(targetAttr)) {
                mappedAttrs.put(targetAttr, newSourceEntityName);
            }
        }

        return mappedAttrs;
    }

    /**
     * Returns the {@code target} attribute name of the first {@code propagateAttributes} entry whose
     * {@code source} is {@link AtlasTypeUtil#ATTRIBUTE_NAME "name"}. The returned {@code target}
     * identifies which {@code {slot}} in the dependent's {@code qualifiedName} template is patched
     * with {@code newName} in {@link #recomputeUniqueAttribute}.
     *
     * @return {@code null} when no mapping has {@code source=name} and a non-blank {@code target}
     */
    private static String findQualifiedNameOverrideKey(List<Map<String, String>> propagateAttributes) {
        if (CollectionUtils.isEmpty(propagateAttributes)) {
            return null;
        }

        for (Map<String, String> mapping : propagateAttributes) {
            String sourceAttr = mapping.get("source");
            String targetAttr = mapping.get("target");

            if (AtlasTypeUtil.ATTRIBUTE_NAME.equals(sourceAttr) && StringUtils.isNotBlank(targetAttr)) {
                return targetAttr;
            }
        }

        return null;
    }

    // ─── qualifiedName recomputation ────────────────────────────────────────────

    /**
     * Recomputes a dependent entity's qualifiedName after a rename using a 3-step in-memory approach:
     * <ol>
     *   <li><b>Parse</b>  — extract each template slot's value from the stored qualifiedName string.</li>
     *   <li><b>Override</b> — replace the single slot that changed (the renamed entity's name key).</li>
     *   <li><b>Rebuild</b> — reconstruct the qualifiedName from the updated slots.</li>
     * </ol>
     * No extra graph reads are required; all values come from the existing qualifiedName string.
     */
    private String recomputeUniqueAttribute(String currentUniqueAttr, String template, String overrideKey, String newValue) {
        Map<String, String> slots = parseUniqueAttribute(currentUniqueAttr, template);

        if (StringUtils.isNotBlank(overrideKey) && newValue != null) {
            slots.put(overrideKey, newValue);
        }

        return buildUniqueAttribute(template, slots);
    }

    /**
     * Parses a qualifiedName string into a {@code { templateKey → value }} map by walking
     * the template and value strings in parallel with two cursors.
     *
     * <p>Literal characters (such as {@code '.'} or {@code '@'}) are delimiters; the character immediately
     * after {@code '}'} in the template is used as the value's end delimiter for each slot.
     * The last slot captures everything remaining in the qualifiedName.
     *
     * <pre>
     *   template   = "{db.name}.{name}@{clusterName}"
     *   uniqueAttr = "mydb.mytable@cluster1"
     *   result     = { "db.name" → "mydb", "name" → "mytable", "clusterName" → "cluster1" }
     * </pre>
     */
    private Map<String, String> parseUniqueAttribute(String uniqueAttr, String template) {
        Map<String, String> slots = new LinkedHashMap<>();
        int                 tPos  = 0; // cursor into template
        int                 uPos  = 0; // cursor into uniqueAttr

        while (tPos < template.length() && uPos <= uniqueAttr.length()) {
            char tc = template.charAt(tPos);

            if (tc != '{') {
                // Literal delimiter — advance both cursors past it.
                tPos++;
                uPos = Math.min(uPos + 1, uniqueAttr.length());
                continue;
            }

            int closeBrace = template.indexOf('}', tPos);

            if (closeBrace < 0) {
                break; // malformed template
            }

            String key = template.substring(tPos + 1, closeBrace);

            tPos = closeBrace + 1; // advance past '}'

            // Last slot: nothing follows in the template, so capture the rest of uniqueAttr.
            if (tPos >= template.length()) {
                slots.put(key, uniqueAttr.substring(uPos));
                break;
            }

            // The character immediately after '}' in the template is the value's end delimiter.
            char delim    = template.charAt(tPos);
            int  delimPos = uniqueAttr.indexOf(delim, uPos);

            if (delimPos >= 0) {
                slots.put(key, uniqueAttr.substring(uPos, delimPos));
                uPos = delimPos + 1; // skip past the delimiter in uniqueAttr
                tPos++;              // skip past the delimiter in template
            } else {
                // Delimiter not found — capture the rest (handles malformed / truncated values).
                slots.put(key, uniqueAttr.substring(uPos));
                uPos = uniqueAttr.length();
            }
        }

        return slots;
    }

    /**
     * Reconstructs a qualifiedName string from a template and a populated slot map.
     * Each {@code {key}} in the template is replaced with its value; unknown keys become {@code ""}.
     *
     * <pre>
     *   template = "{db.name}.{name}@{clusterName}"
     *   slots    = { "db.name" → "mydb", "name" → "new_table", "clusterName" → "cluster1" }
     *   result   = "mydb.new_table@cluster1"
     * </pre>
     */
    private String buildUniqueAttribute(String template, Map<String, String> slots) {
        StringBuilder sb   = new StringBuilder(template.length() + 32);
        int           tPos = 0;

        while (tPos < template.length()) {
            if (template.charAt(tPos) != '{') {
                sb.append(template.charAt(tPos++));
                continue;
            }

            int closeBrace = template.indexOf('}', tPos);

            if (closeBrace < 0) {
                break; // malformed template
            }

            String key = template.substring(tPos + 1, closeBrace);

            sb.append(slots.getOrDefault(key, ""));

            tPos = closeBrace + 1;
        }

        return sb.toString();
    }

    // ─── small helpers ──────────────────────────────────────────────────────────

    /** Returns the {@code autoComputeFormat} for the entity's {@code qualifiedName} attribute, trimmed. */
    private String getUniqueAttributeTemplate(AtlasEntityType entityType) {
        AtlasAttribute uniqueAttr = entityType.getAttribute(AtlasTypeUtil.ATTRIBUTE_QUALIFIED_NAME);

        if (uniqueAttr == null || uniqueAttr.getAttributeDef() == null) {
            return null;
        }

        String fmt = uniqueAttr.getAttributeDef().getAutoComputeFormat();

        return fmt != null ? fmt.trim() : null;
    }

    private AtlasVertex getOtherVertex(AtlasEdge edge, AtlasVertex anchor) {
        AtlasVertex out = edge.getOutVertex();
        AtlasVertex in  = edge.getInVertex();

        return out != null && out.equals(anchor) ? in : out;
    }

    private <T> List<T> toList(Iterator<T> iterator) {
        List<T> list = new ArrayList<>();

        if (iterator != null) {
            while (iterator.hasNext()) {
                list.add(iterator.next());
            }
        }

        return list;
    }

    // ─── inner class ────────────────────────────────────────────────────────────

    public static class DependentUpdate {
        private final String              guid;
        private final String              typeName;
        private final AtlasVertex         vertex;
        private final String              newUniqueAttribute;
        private final Map<String, Object> mappedAttrs;

        public DependentUpdate(String guid, String typeName, AtlasVertex vertex, String newUniqueAttribute, Map<String, Object> mappedAttrs) {
            this.guid               = guid;
            this.typeName           = typeName;
            this.vertex             = vertex;
            this.newUniqueAttribute = newUniqueAttribute;
            this.mappedAttrs        = mappedAttrs;
        }

        public String getGuid() {
            return guid;
        }

        public String getTypeName() {
            return typeName;
        }

        public AtlasVertex getVertex() {
            return vertex;
        }

        public String getNewUniqueAttribute() {
            return newUniqueAttribute;
        }

        public Map<String, Object> getMappedAttrs() {
            return mappedAttrs;
        }
    }
}
