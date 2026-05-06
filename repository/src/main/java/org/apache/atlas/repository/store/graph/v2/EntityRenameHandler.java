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

    /** Key for trigger-side attribute in relationship end {@code propagateAttributes} map entries. */
    private static final String  PROPAGATE_ATTRIBUTES_SOURCE_KEY = "source";
    /** Key for dependent stub attribute in those entries. */
    private static final String  PROPAGATE_ATTRIBUTES_TARGET_KEY = "target";

    private final AtlasTypeRegistry typeRegistry;

    @Inject
    public EntityRenameHandler(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    /**
     * Discovers all entities whose qualifiedName is impacted by the given rename and injects
     * them into the mutation context so they are persisted in the same transaction.
     *
     * Uses typedef-time metadata on each {@link AtlasEntityType} for O(1) slot-key lookup where
     * applicable, without resolving qualifiedName patterns from the graph at runtime.
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

        LOG.debug("addDependentsToContext(): done — renamedType={}, dependentCount={}", renamedTypeName, dependents.size());

        injectDependentsIntoContext(context, dependents);
    }

    /**
     * Registers each dependent rewrite on {@code context} so they persist in the same transaction as the
     * renamed root entity.
     * <p>
     * For every entry in {@code dependents}, builds a minimal {@link AtlasEntity} (GUID, new
     * {@code qualifiedName}, and any extra attributes from {@code propagateAttributes}), resolves the
     * dependent's {@link AtlasEntityType}, and calls {@link EntityMutationContext#addUpdated}.
     *
     * @param context     mutation batch for the current create/update
     * @param dependents  dependent entity GUID → payload produced by {@link #collectDependents}
     */
    private void injectDependentsIntoContext(EntityMutationContext context, Map<String, DependentUpdate> dependents) {
        for (DependentUpdate dependentUpdate : dependents.values()) {
            AtlasEntity dependentStub = new AtlasEntity(dependentUpdate.getTypeName());

            dependentStub.setGuid(dependentUpdate.getGuid());
            dependentStub.setAttribute(AtlasTypeUtil.ATTRIBUTE_QUALIFIED_NAME, dependentUpdate.getNewUniqueAttribute());

            if (MapUtils.isNotEmpty(dependentUpdate.getMappedAttrs())) {
                for (Map.Entry<String, Object> mappedAttrEntry : dependentUpdate.getMappedAttrs().entrySet()) {
                    dependentStub.setAttribute(mappedAttrEntry.getKey(), mappedAttrEntry.getValue());
                }
            }

            AtlasEntityType dependentEntityType = typeRegistry.getEntityTypeByName(dependentUpdate.getTypeName());

            context.addUpdated(dependentUpdate.getGuid(), dependentStub, dependentEntityType, dependentUpdate.getVertex());
        }
    }

    /**
     * Walks {@link RenamePropagationTarget}s declared on {@code sourceEntityType}, follows each relationship
     * from {@code sourceVertex} to neighboring entities, recomputes each neighbor's {@code qualifiedName} when it
     * should reflect {@code renamedTypeName}'s new {@code newName}, and records updates in
     * {@code dependentUpdatesByGuid}. Recurses when a neighbor's type also declares propagation targets.
     *
     * <p>How the segment of {@code qualifiedName} to rewrite is chosen:
     * <ul>
     *   <li>When {@code propagateAttributes} is present: use {@link #findQualifiedNameOverrideKey} (mapping whose
     *       {@code source} is {@code "name"}), write {@code newName} into that {@code target} path, and add any
     *       extra attributes via {@link #buildMappedAttrs}. For the next level, {@code newName} is read back from
     *       the patched path on the new qualified name (falling back to the prior {@code newName} if blank).</li>
     *   <li>When {@code propagateAttributes} is absent: use {@link AtlasEntityType#getAutoComputeFormatPathByRefTypeNameMap()}
     *       on the dependent type to find the dotted path for {@code renamedTypeName}, then recompute with the same
     *       {@code newName}. Recursion keeps the same {@code renamedTypeName} and {@code newName}.</li>
     * </ul>
     *
     * @param sourceVertex              graph vertex for the entity currently acting as the rename root
     * @param sourceEntityType          typedef for {@code sourceVertex}
     * @param renamedTypeName           entity type whose name change drives this step (used for map lookup when there is no {@code propagateAttributes})
     * @param newName                   replacement {@code name} (or a value derived from the patched qualified name when {@code propagateAttributes} is used)
     * @param visitedGuids              prevents revisiting the same dependent GUID in a cycle
     * @param dependentUpdatesByGuid    accumulates dependent GUID → {@link DependentUpdate}
     */
    private void collectDependents(AtlasVertex sourceVertex, AtlasEntityType sourceEntityType,
                                   String renamedTypeName, String newName,
                                   Set<String> visitedGuids, Map<String, DependentUpdate> dependentUpdatesByGuid) throws AtlasBaseException {
        for (RenamePropagationTarget propagationTarget : sourceEntityType.getRenamePropagationTargets()) {
            AtlasAttribute relAttr = propagationTarget.getRelAttr();

            if (relAttr == null) {
                continue;
            }

            for (AtlasEdge edge : toList(GraphHelper.getEdgesForLabel(sourceVertex, relAttr.getRelationshipEdgeLabel(), relAttr.getRelationshipEdgeDirection()))) {
                if (GraphHelper.getStatus(edge) == AtlasEntity.Status.DELETED) {
                    LOG.debug("collectDependents(): skip — soft-deleted edge (label={}, relationshipType={}, relationshipGuid={})",
                            relAttr.getRelationshipEdgeLabel(),
                            AtlasGraphUtilsV2.getTypeName(edge),
                            GraphHelper.getRelationshipGuid(edge));
                    continue;
                }

                AtlasVertex dependentVertex = getOtherVertex(edge, sourceVertex);

                if (dependentVertex == null) {
                    continue;
                }

                if (GraphHelper.getStatus(dependentVertex) == AtlasEntity.Status.DELETED) {
                    LOG.debug("collectDependents(): skip — soft-deleted dependent vertex (guid={})", AtlasGraphUtilsV2.getIdFromVertex(dependentVertex));
                    continue;
                }

                String dependentGuid = AtlasGraphUtilsV2.getIdFromVertex(dependentVertex);

                if (StringUtils.isBlank(dependentGuid) || !visitedGuids.add(dependentGuid)) {
                    continue;
                }

                String          dependentTypeName   = GraphHelper.getTypeName(dependentVertex);
                AtlasEntityType dependentEntityType = typeRegistry.getEntityTypeByName(dependentTypeName);

                if (dependentEntityType == null) {
                    LOG.debug("collectDependents(): skip — no typedef for dependent type '{}' (guid={})", dependentTypeName, dependentGuid);
                    continue;
                }

                String uniqueAttributeAutoComputeFormat = getUniqueAttributeAutoComputeFormat(dependentEntityType);
                String oldQualifiedName = AtlasGraphUtilsV2.getProperty(dependentVertex,
                        dependentEntityType.getVertexPropertyName(AtlasTypeUtil.ATTRIBUTE_QUALIFIED_NAME), String.class);

                if (StringUtils.isBlank(uniqueAttributeAutoComputeFormat) || StringUtils.isBlank(oldQualifiedName)) {
                    LOG.debug("collectDependents(): skip — blank unique-attribute autoComputeFormat or stored qualifiedName (dependentType={}; guid={})",
                            dependentTypeName, dependentGuid);
                    continue;
                }

                String qualifiedNameOverrideKey;
                Map<String, Object> mappedAttrs;

                if (CollectionUtils.isNotEmpty(propagationTarget.getPropagateAttributes())) {
                    // Dotted path comes from propagateAttributes (entry whose source is "name").
                    qualifiedNameOverrideKey = findQualifiedNameOverrideKey(propagationTarget.getPropagateAttributes());
                    if (qualifiedNameOverrideKey == null) {
                        qualifiedNameOverrideKey = AtlasTypeUtil.ATTRIBUTE_NAME;
                    }
                    mappedAttrs = buildMappedAttrs(propagationTarget, newName);
                } else {
                    // Typedef map: referenced type → dotted path in this dependent's qualifiedName autoComputeFormat.
                    qualifiedNameOverrideKey = dependentEntityType.getAutoComputeFormatPathByRefTypeNameMap().get(renamedTypeName);

                    if (qualifiedNameOverrideKey == null) {
                        LOG.debug("collectDependents(): no qualifiedName path for renamedType={} on dependentType={} — skipping",
                                renamedTypeName, dependentTypeName);
                        continue;
                    }

                    mappedAttrs = new HashMap<>();
                }

                String newQualifiedName = recomputeUniqueAttribute(oldQualifiedName, uniqueAttributeAutoComputeFormat, qualifiedNameOverrideKey, newName);

                if (StringUtils.isBlank(newQualifiedName) || StringUtils.equals(oldQualifiedName, newQualifiedName)) {
                    LOG.debug("collectDependents(): skip — recomputed qualifiedName empty or unchanged (dependentType={}; guid={})",
                            dependentTypeName, dependentGuid);
                    continue;
                }

                LOG.debug("collectDependents(): dependent qualifiedName update type={} guid={} {} -> {}",
                        dependentTypeName, dependentGuid, oldQualifiedName, newQualifiedName);

                dependentUpdatesByGuid.put(dependentGuid, new DependentUpdate(dependentGuid, dependentTypeName, dependentVertex, newQualifiedName, mappedAttrs));

                if (CollectionUtils.isNotEmpty(dependentEntityType.getRenamePropagationTargets())) {
                    if (CollectionUtils.isNotEmpty(propagationTarget.getPropagateAttributes())) {
                        // This dependent becomes the next root; recurse with the propagated name so
                        // further dependents are updated relative to this entity's new name.
                        collectDependents(dependentVertex, dependentEntityType, dependentTypeName, newName, visitedGuids, dependentUpdatesByGuid);
                    } else {
                        // Same renamed type and name propagate to further dependents.
                        collectDependents(dependentVertex, dependentEntityType, renamedTypeName, newName, visitedGuids, dependentUpdatesByGuid);
                    }
                }
            }
        }
    }

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
            String sourceAttr = mapping.get(PROPAGATE_ATTRIBUTES_SOURCE_KEY);
            String targetAttr = mapping.get(PROPAGATE_ATTRIBUTES_TARGET_KEY);

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
     * identifies which dotted path in the dependent's unique-attribute {@code autoComputeFormat} is patched
     * with {@code newName} in {@link #recomputeUniqueAttribute}.
     *
     * @return {@code null} when no mapping has {@code source=name} with a non-blank {@code target}
     */
    private static String findQualifiedNameOverrideKey(List<Map<String, String>> propagateAttributes) {
        if (CollectionUtils.isEmpty(propagateAttributes)) {
            return null;
        }

        for (Map<String, String> mapping : propagateAttributes) {
            String sourceAttr = mapping.get(PROPAGATE_ATTRIBUTES_SOURCE_KEY);
            String targetAttr = mapping.get(PROPAGATE_ATTRIBUTES_TARGET_KEY);

            if (AtlasTypeUtil.ATTRIBUTE_NAME.equals(sourceAttr) && StringUtils.isNotBlank(targetAttr)) {
                return targetAttr;
            }
        }

        return null;
    }

    /**
     * Recomputes a dependent entity's qualifiedName after a rename using a 3-step in-memory approach:
     * <ol>
     *   <li>{@code Parse}: split the stored qualifiedName using the unique attribute's {@code autoComputeFormat}.</li>
     *   <li>{@code Override}: replace the path segment that changed (the renamed entity's name key).</li>
     *   <li>{@code Rebuild}: stitch the qualifiedName back together from the updated segments.</li>
     * </ol>
     * No extra graph reads are required; all values come from the existing qualifiedName string.
     */
    private String recomputeUniqueAttribute(String currentUniqueAttr, String uniqueAttributeAutoComputeFormat, String overrideKey, String newValue) {
        Map<String, String> slots = parseUniqueAttribute(currentUniqueAttr, uniqueAttributeAutoComputeFormat);

        if (StringUtils.isNotBlank(overrideKey) && newValue != null) {
            slots.put(overrideKey, newValue);
        }

        return buildUniqueAttribute(uniqueAttributeAutoComputeFormat, slots);
    }

    /**
     * Parses a qualifiedName string into a {@code { pathKey → value }} map by walking the unique attribute's
     * {@code autoComputeFormat} pattern and the stored value in parallel.
     *
     * <p>Literal characters (such as {@code '.'} or {@code '@'}) are delimiters; the character immediately
     * after {@code '}'} in the format string is used as the value's end delimiter for each placeholder.
     * The last placeholder captures everything remaining in the qualifiedName.
     *
     * <pre>
     *   uniqueAttributeAutoComputeFormat = "{db.name}.{name}@{clusterName}"
     *   uniqueAttr                       = "mydb.mytable@cluster1"
     *   result                           = { "db.name" → "mydb", "name" → "mytable", "clusterName" → "cluster1" }
     * </pre>
     */
    private Map<String, String> parseUniqueAttribute(String uniqueAttr, String uniqueAttributeAutoComputeFormat) {
        Map<String, String> slots = new LinkedHashMap<>();
        int                 tPos  = 0; // cursor into uniqueAttributeAutoComputeFormat
        int                 uPos  = 0; // cursor into uniqueAttr

        while (tPos < uniqueAttributeAutoComputeFormat.length() && uPos <= uniqueAttr.length()) {
            char tc = uniqueAttributeAutoComputeFormat.charAt(tPos);

            if (tc != '{') {
                // Literal delimiter — advance both cursors past it.
                tPos++;
                uPos = Math.min(uPos + 1, uniqueAttr.length());
                continue;
            }

            int closeBrace = uniqueAttributeAutoComputeFormat.indexOf('}', tPos);

            if (closeBrace < 0) {
                break; // malformed autoComputeFormat
            }

            String key = uniqueAttributeAutoComputeFormat.substring(tPos + 1, closeBrace);

            tPos = closeBrace + 1; // advance past '}'

            // Last placeholder: nothing follows in the format string, so capture the rest of uniqueAttr.
            if (tPos >= uniqueAttributeAutoComputeFormat.length()) {
                slots.put(key, uniqueAttr.substring(uPos));
                break;
            }

            // Character after '}' in the format string is the value's end delimiter in uniqueAttr.
            char delim    = uniqueAttributeAutoComputeFormat.charAt(tPos);
            int  delimPos = uniqueAttr.indexOf(delim, uPos);

            if (delimPos >= 0) {
                slots.put(key, uniqueAttr.substring(uPos, delimPos));
                uPos = delimPos + 1; // skip past the delimiter in uniqueAttr
                tPos++;              // skip past the delimiter in format string
            } else {
                // Delimiter not found — capture the rest (handles malformed / truncated values).
                slots.put(key, uniqueAttr.substring(uPos));
                uPos = uniqueAttr.length();
            }
        }

        return slots;
    }

    /**
     * Reconstructs a qualifiedName string from the unique attribute's {@code autoComputeFormat} and a populated map.
     * Each {@code {key}} in the format string is replaced with its value; unknown keys become {@code ""}.
     *
     * <pre>
     *   uniqueAttributeAutoComputeFormat = "{db.name}.{name}@{clusterName}"
     *   slots                          = { "db.name" → "mydb", "name" → "new_table", "clusterName" → "cluster1" }
     *   result                         = "mydb.new_table@cluster1"
     * </pre>
     */
    private String buildUniqueAttribute(String uniqueAttributeAutoComputeFormat, Map<String, String> slots) {
        StringBuilder sb   = new StringBuilder(uniqueAttributeAutoComputeFormat.length() + 32);
        int           tPos = 0;

        while (tPos < uniqueAttributeAutoComputeFormat.length()) {
            if (uniqueAttributeAutoComputeFormat.charAt(tPos) != '{') {
                sb.append(uniqueAttributeAutoComputeFormat.charAt(tPos++));
                continue;
            }

            int closeBrace = uniqueAttributeAutoComputeFormat.indexOf('}', tPos);

            if (closeBrace < 0) {
                break; // malformed autoComputeFormat
            }

            String key = uniqueAttributeAutoComputeFormat.substring(tPos + 1, closeBrace);

            sb.append(slots.getOrDefault(key, ""));

            tPos = closeBrace + 1;
        }

        return sb.toString();
    }

    /** Returns trimmed {@code autoComputeFormat} for the type's unique ({@code qualifiedName}) attribute. */
    private String getUniqueAttributeAutoComputeFormat(AtlasEntityType entityType) {
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
