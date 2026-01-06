/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.store.graph.v2;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.apache.atlas.type.Constants.MEANINGS_PROPERTY_KEY;
import static org.apache.atlas.type.Constants.MEANINGS_TEXT_PROPERTY_KEY;
import static org.apache.atlas.type.Constants.MEANING_NAMES_PROPERTY_KEY;

/**
 * Helper class responsible for updating the __meaningsText property on graph vertices.
 * This class encapsulates all the logic for handling both full replace and append modes,
 * ensuring that the __meaningsText property accurately reflects the final state of meanings
 * after additions and deletions.
 */
public class MeaningsTextPropertyUpdater {

    /**
     * Updates the __meaningsText property on the referring vertex based on the meanings changes.
     * This method handles both full replace and append modes.
     *
     * @param ctx                      The attribute mutation context containing the referring vertex
     * @param isAppend                 Whether the operation is in append mode or full replace mode
     * @param newMeaningNames          List of names of newly added meanings
     * @param deletedMeaningsNames     List of names of deleted meanings
     * @param newMeaningsQNames        Set of qualified names of newly added meanings
     * @param deletedMeaningsQNames    List of qualified names of deleted meanings
     */
    public void update(AttributeMutationContext ctx, boolean isAppend,
                      List<String> newMeaningNames, List<String> deletedMeaningsNames,
                      Set<String> newMeaningsQNames, List<String> deletedMeaningsQNames) {
        if (!isAppend) {
            handleFullReplaceMeaningsText(ctx, newMeaningNames);
        } else {
            handleAppendMeaningsText(ctx, newMeaningNames, deletedMeaningsNames, 
                                    newMeaningsQNames, deletedMeaningsQNames);
        }
    }

    /**
     * Handles full replace mode for meanings text property.
     * Simply sets the property to the new meaning names.
     */
    private void handleFullReplaceMeaningsText(AttributeMutationContext ctx, List<String> newMeaningNames) {
        if (CollectionUtils.isNotEmpty(newMeaningNames)) {
            AtlasGraphUtilsV2.setEncodedProperty(ctx.referringVertex, MEANINGS_TEXT_PROPERTY_KEY, 
                StringUtils.join(newMeaningNames, ","));
        }
    }

    /**
     * Handles append mode for meanings text property.
     * Recalculates the text based on remaining meanings after additions and deletions.
     */
    private void handleAppendMeaningsText(AttributeMutationContext ctx, List<String> newMeaningNames,
                                         List<String> deletedMeaningsNames, Set<String> newMeaningsQNames,
                                         List<String> deletedMeaningsQNames) {
        boolean hasChanges = CollectionUtils.isNotEmpty(deletedMeaningsQNames) || 
                            CollectionUtils.isNotEmpty(newMeaningsQNames);
        
        if (hasChanges) {
            handleAppendWithChanges(ctx, newMeaningNames, deletedMeaningsNames);
        } else if (CollectionUtils.isNotEmpty(newMeaningNames)) {
            handleAppendOnlyAdditions(ctx, newMeaningNames);
        }
    }

    /**
     * Handles append mode when there are additions and/or deletions.
     * Rebuilds the meanings text from remaining meanings.
     */
    private void handleAppendWithChanges(AttributeMutationContext ctx, List<String> newMeaningNames,
                                        List<String> deletedMeaningsNames) {
        List<String> remainingMeaningsQNames = ctx.getReferringVertex()
            .getMultiValuedProperty(MEANINGS_PROPERTY_KEY, String.class);

        if (CollectionUtils.isEmpty(remainingMeaningsQNames)) {
            clearMeaningsTextProperty(ctx);
        } else {
            Set<String> finalNames = buildFinalMeaningNames(ctx, newMeaningNames, deletedMeaningsNames);
            setMeaningsTextProperty(ctx, finalNames);
        }
    }

    /**
     * Handles append mode when there are only additions (no deletions).
     * Appends new names to existing text without rebuilding.
     */
    private void handleAppendOnlyAdditions(AttributeMutationContext ctx, List<String> newMeaningNames) {
        String existingText = AtlasGraphUtilsV2.getProperty(ctx.referringVertex, 
            MEANINGS_TEXT_PROPERTY_KEY, String.class);
        
        if (StringUtils.isNotEmpty(existingText)) {
            AtlasGraphUtilsV2.setEncodedProperty(ctx.referringVertex, MEANINGS_TEXT_PROPERTY_KEY, 
                existingText + "," + StringUtils.join(newMeaningNames, ","));
        } else {
            AtlasGraphUtilsV2.setEncodedProperty(ctx.referringVertex, MEANINGS_TEXT_PROPERTY_KEY, 
                StringUtils.join(newMeaningNames, ","));
        }
    }

    /**
     * Builds the final set of meaning names by combining new and existing names,
     * excluding deleted names. Optimized to only fetch existing names when there are deletions.
     * Uses LinkedHashSet to maintain deterministic ordering (existing names first, then new names).
     */
    private Set<String> buildFinalMeaningNames(AttributeMutationContext ctx, List<String> newMeaningNames,
                                              List<String> deletedMeaningsNames) {
        Set<String> finalNames = new LinkedHashSet<>();
        
        // Add existing names first (to preserve original order), conditionally fetching based on whether there are deletions
        if (CollectionUtils.isNotEmpty(deletedMeaningsNames)) {
            addExistingNamesExcludingDeleted(ctx, finalNames, deletedMeaningsNames);
        } else {
            addAllExistingNames(ctx, finalNames);
        }
        
        // Add new names after existing (append semantics)
        if (CollectionUtils.isNotEmpty(newMeaningNames)) {
            finalNames.addAll(newMeaningNames);
        }
        
        return finalNames;
    }

    /**
     * Adds existing meaning names to the final set, excluding those that were deleted.
     * Only called when there are deletions (optimization).
     */
    private void addExistingNamesExcludingDeleted(AttributeMutationContext ctx, Set<String> finalNames,
                                                  List<String> deletedMeaningsNames) {
        List<String> currentMeaningNames = ctx.getReferringVertex()
            .getMultiValuedProperty(MEANING_NAMES_PROPERTY_KEY, String.class);
        
        if (CollectionUtils.isNotEmpty(currentMeaningNames)) {
            for (String name : currentMeaningNames) {
                if (!deletedMeaningsNames.contains(name)) {
                    finalNames.add(name);
                }
            }
        }
    }

    /**
     * Adds all existing meaning names to the final set.
     * Only called when there are no deletions (optimization).
     */
    private void addAllExistingNames(AttributeMutationContext ctx, Set<String> finalNames) {
        List<String> currentMeaningNames = ctx.getReferringVertex()
            .getMultiValuedProperty(MEANING_NAMES_PROPERTY_KEY, String.class);
        
        if (CollectionUtils.isNotEmpty(currentMeaningNames)) {
            finalNames.addAll(currentMeaningNames);
        }
    }

    /**
     * Sets the meanings text property from a set of names, or clears it if empty.
     */
    private void setMeaningsTextProperty(AttributeMutationContext ctx, Set<String> finalNames) {
        if (CollectionUtils.isNotEmpty(finalNames)) {
            AtlasGraphUtilsV2.setEncodedProperty(ctx.referringVertex, MEANINGS_TEXT_PROPERTY_KEY, 
                StringUtils.join(finalNames, ","));
        } else {
            clearMeaningsTextProperty(ctx);
        }
    }

    /**
     * Clears the meanings text property from the vertex.
     */
    private void clearMeaningsTextProperty(AttributeMutationContext ctx) {
        ctx.getReferringVertex().removeProperty(MEANINGS_TEXT_PROPERTY_KEY);
    }
}
