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

package org.apache.atlas.repository.util;

import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.repository.store.graph.v2.ClassificationAssociator.Updater.PROCESS_ADD;
import static org.apache.atlas.repository.store.graph.v2.ClassificationAssociator.Updater.PROCESS_DELETE;
import static org.apache.atlas.repository.store.graph.v2.ClassificationAssociator.Updater.PROCESS_NOOP;
import static org.apache.atlas.repository.store.graph.v2.ClassificationAssociator.Updater.PROCESS_UPDATE;

public final class AtlasEntityUtilsTestAppend {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasEntityUtilsTestAppend.class);


    public static void main(String[] args) {
        allEmpty();

        addNewOnly();
        removeAll();
        updateAll();

        noUpdate();

        oneUpdateOneNOOP();
        oneUpdateOneAddOneNOOP();
        oneUpdateOneAddOneRemoveOneNOOP();

        currentOneAddNew();
        currentOneAddNewDedup();
        addDup();
        updateDup();
        NOOPDup();
    }

    private static void allEmpty() {
        List<AtlasClassification> currentTags = new ArrayList<>();
        List<AtlasClassification> newTags = new ArrayList<>();
        List<AtlasClassification> removeTags = new ArrayList<>();

        Map<String, List<AtlasClassification>> diff = AtlasEntityUtils.getTagsDiffForAppend("123",
                newTags,
                currentTags,
                removeTags);
        assert MapUtils.isEmpty(diff);
    }


    private static void addNewOnly() {
        List<AtlasClassification> newTags = new ArrayList<>();
        newTags.add(getTag("alpha"));
        newTags.add(getTag("beta"));

        List<AtlasClassification> removeTags = new ArrayList<>();

        Map<String, List<AtlasClassification>> diff = AtlasEntityUtils.getTagsDiffForAppend("123",
                newTags,
                new ArrayList<>(),
                removeTags);
        assert MapUtils.isNotEmpty(diff);

        assert diff.containsKey(PROCESS_ADD);
        assert !diff.containsKey(PROCESS_DELETE);
        assert !diff.containsKey(PROCESS_UPDATE);
        assert !diff.containsKey(PROCESS_NOOP);

        List<AtlasClassification> resultAdd = diff.get(PROCESS_ADD);
        assert resultAdd.size() == 2;

        assert getFinalList(diff).size() == 2;
    }

    private static void removeAll() {
        List<AtlasClassification> currentTags = new ArrayList<>();
        currentTags.add(getTag("alpha"));
        currentTags.add(getTag("beta"));

        List<AtlasClassification> newTags = new ArrayList<>();

        List<AtlasClassification> removeTags = new ArrayList<>();
        removeTags.add(getTag("alpha"));
        removeTags.add(getTag("beta"));

        Map<String, List<AtlasClassification>> diff = AtlasEntityUtils.getTagsDiffForAppend("123", newTags, currentTags, removeTags);

        assert diff.containsKey(PROCESS_DELETE);
        assert !diff.containsKey(PROCESS_ADD);
        assert !diff.containsKey(PROCESS_UPDATE);
        assert !diff.containsKey(PROCESS_NOOP);

        assert diff.get(PROCESS_DELETE).size() == 2;

        assert getFinalList(diff).size() == 0;
    }

    private static void updateAll() {
        List<AtlasClassification> currentTags = new ArrayList<>();
        currentTags.add(getTag("alpha", "123", false));
        currentTags.add(getTag("beta", "123", false));

        List<AtlasClassification> newTags = new ArrayList<>();
        newTags.add(getTag("alpha", true));
        newTags.add(getTag("beta", true));

        List<AtlasClassification> removeTags = new ArrayList<>();

        Map<String, List<AtlasClassification>> diff = AtlasEntityUtils.getTagsDiffForAppend("123", newTags, currentTags, removeTags);

        assert diff.containsKey(PROCESS_UPDATE);
        assert !diff.containsKey(PROCESS_DELETE);
        assert !diff.containsKey(PROCESS_ADD);
        assert !diff.containsKey(PROCESS_NOOP);

        assert diff.get(PROCESS_UPDATE).size() == 2;

        assert getFinalList(diff).size() == 2;
    }

    private static void noUpdate() {
        List<AtlasClassification> currentTags = new ArrayList<>();
        currentTags.add(getTag("alpha", "123", true));
        currentTags.add(getTag("beta", "123", true));

        List<AtlasClassification> newTags = new ArrayList<>();
        newTags.add(getTag("alpha", true));
        newTags.add(getTag("beta", true));

        List<AtlasClassification> removeTags = new ArrayList<>();

        Map<String, List<AtlasClassification>> diff = AtlasEntityUtils.getTagsDiffForAppend("123", newTags, currentTags,removeTags);

        assert diff.containsKey(PROCESS_NOOP);
        assert !diff.containsKey(PROCESS_UPDATE);
        assert !diff.containsKey(PROCESS_DELETE);
        assert !diff.containsKey(PROCESS_ADD);

        assert diff.get(PROCESS_NOOP).size() == 2;
        assert getFinalList(diff).size() == 2;
    }

    private static void oneUpdateOneNOOP() {
        List<AtlasClassification> currentTags = new ArrayList<>();
        currentTags.add(getTag("alpha", "123", true));
        currentTags.add(getTag("beta", "123", true));

        List<AtlasClassification> newTags = new ArrayList<>();
        newTags.add(getTag("alpha", true));
        newTags.add(getTag("beta", false));

        List<AtlasClassification> removeTags = new ArrayList<>();

        Map<String, List<AtlasClassification>> diff = AtlasEntityUtils.getTagsDiffForAppend("123", newTags, currentTags, removeTags);

        assert diff.containsKey(PROCESS_NOOP);
        assert diff.containsKey(PROCESS_UPDATE);
        assert !diff.containsKey(PROCESS_DELETE);
        assert !diff.containsKey(PROCESS_ADD);

        assert diff.get(PROCESS_NOOP).size() == 1;
        assert diff.get(PROCESS_NOOP).get(0).getTypeName().equals("alpha");

        assert diff.get(PROCESS_UPDATE).size() == 1;
        assert diff.get(PROCESS_UPDATE).get(0).getTypeName().equals("beta");

        assert getFinalList(diff).size() == 2;
    }

    private static void oneUpdateOneAddOneNOOP() {
        List<AtlasClassification> currentTags = new ArrayList<>();
        currentTags.add(getTag("alpha", "123", true));
        currentTags.add(getTag("beta", "123", true));

        List<AtlasClassification> newTags = new ArrayList<>();
        newTags.add(getTag("alpha", true));
        newTags.add(getTag("beta", false));
        newTags.add(getTag("gamma", false));

        List<AtlasClassification> removeTags = new ArrayList<>();

        Map<String, List<AtlasClassification>> diff = AtlasEntityUtils.getTagsDiffForAppend("123", newTags, currentTags, removeTags);

        assert diff.containsKey(PROCESS_NOOP);
        assert diff.containsKey(PROCESS_UPDATE);
        assert diff.containsKey(PROCESS_ADD);
        assert !diff.containsKey(PROCESS_DELETE);

        assert diff.get(PROCESS_NOOP).size() == 1;
        assert diff.get(PROCESS_NOOP).get(0).getTypeName().equals("alpha");

        assert diff.get(PROCESS_UPDATE).size() == 1;
        assert diff.get(PROCESS_UPDATE).get(0).getTypeName().equals("beta");

        assert diff.get(PROCESS_ADD).size() == 1;
        assert diff.get(PROCESS_ADD).get(0).getTypeName().equals("gamma");

        assert getFinalList(diff).size() == 3;
    }

    private static void oneUpdateOneAddOneRemoveOneNOOP() {
        List<AtlasClassification> currentTags = new ArrayList<>();
        currentTags.add(getTag("alpha", "123", true));
        currentTags.add(getTag("beta", "123", true));
        currentTags.add(getTag("gamma", "123", true));

        List<AtlasClassification> newTags = new ArrayList<>();
        newTags.add(getTag("alpha", true));
        newTags.add(getTag("beta", false));
        newTags.add(getTag("x-ray", "123", true));

        List<AtlasClassification> removeTags = new ArrayList<>();
        removeTags.add(getTag("gamma", "123", true));

        Map<String, List<AtlasClassification>> diff = AtlasEntityUtils.getTagsDiffForAppend("123", newTags, currentTags, removeTags);

        assert diff.containsKey(PROCESS_NOOP);
        assert diff.containsKey(PROCESS_UPDATE);
        assert diff.containsKey(PROCESS_ADD);
        assert diff.containsKey(PROCESS_DELETE);

        assert diff.get(PROCESS_NOOP).size() == 1;
        assert diff.get(PROCESS_NOOP).get(0).getTypeName().equals("alpha");

        assert diff.get(PROCESS_UPDATE).size() == 1;
        assert diff.get(PROCESS_UPDATE).get(0).getTypeName().equals("beta");

        assert diff.get(PROCESS_ADD).size() == 1;
        assert diff.get(PROCESS_ADD).get(0).getTypeName().equals("x-ray");

        assert diff.get(PROCESS_DELETE).size() == 1;
        assert diff.get(PROCESS_DELETE).get(0).getTypeName().equals("gamma");

        assert getFinalList(diff).size() == 3;
    }

    private static void currentOneAddNew() {
        List<AtlasClassification> currentTags = new ArrayList<>();
        currentTags.add(getTag("alpha", "123"));
        currentTags.add(getTag("beta", "123"));

        List<AtlasClassification> newTags = new ArrayList<>();
        newTags.add(getTag("gamma"));

        List<AtlasClassification> removeTags = new ArrayList<>();

        Map<String, List<AtlasClassification>> diff = AtlasEntityUtils.getTagsDiffForAppend("123", newTags, currentTags, removeTags);

        assert diff.containsKey(PROCESS_ADD);
        assert diff.containsKey(PROCESS_NOOP);
        assert !diff.containsKey(PROCESS_DELETE);
        assert !diff.containsKey(PROCESS_UPDATE);

        assert diff.get(PROCESS_ADD).size() == 1;
        assert diff.get(PROCESS_NOOP).size() == 2;

        assert getFinalList(diff).size() == 3;
    }

    private static void currentOneAddNewDedup() {
        List<AtlasClassification> currentTags = new ArrayList<>();
        currentTags.add(getTag("alpha", "123"));
        currentTags.add(getTag("beta", "123"));

        List<AtlasClassification> newTags = new ArrayList<>();
        newTags.add(getTag("alpha"));
        newTags.add(getTag("beta"));
        newTags.add(getTag("gamma"));

        List<AtlasClassification> removeTags = new ArrayList<>();

        Map<String, List<AtlasClassification>> diff = AtlasEntityUtils.getTagsDiffForAppend("123", newTags, currentTags, removeTags);

        assert diff.containsKey(PROCESS_ADD);
        assert diff.containsKey(PROCESS_NOOP);
        assert !diff.containsKey(PROCESS_DELETE);
        assert !diff.containsKey(PROCESS_UPDATE);

        assert diff.get(PROCESS_ADD).size() == 1;
        assert diff.get(PROCESS_NOOP).size() == 2;

        assert getFinalList(diff).size() == 3;
    }

    private static void addDup() {
        List<AtlasClassification> currentTags = new ArrayList<>();

        List<AtlasClassification> newTags = new ArrayList<>();
        newTags.add(getTag("alpha"));
        newTags.add(getTag("alpha"));

        List<AtlasClassification> removeTags = new ArrayList<>();

        Map<String, List<AtlasClassification>> diff = AtlasEntityUtils.getTagsDiffForAppend("123", newTags, currentTags, removeTags);

        assert diff.containsKey(PROCESS_ADD);
        assert !diff.containsKey(PROCESS_NOOP);
        assert !diff.containsKey(PROCESS_DELETE);
        assert !diff.containsKey(PROCESS_UPDATE);

        assert diff.get(PROCESS_ADD).size() == 1;
        assert getFinalList(diff).size() == 1;
    }

    private static void updateDup() {
        List<AtlasClassification> currentTags = new ArrayList<>();
        currentTags.add(getTag("alpha", "123", false));

        List<AtlasClassification> newTags = new ArrayList<>();
        newTags.add(getTag("alpha", true));
        newTags.add(getTag("alpha", true));

        List<AtlasClassification> removeTags = new ArrayList<>();

        Map<String, List<AtlasClassification>> diff = AtlasEntityUtils.getTagsDiffForAppend("123", newTags, currentTags, removeTags);

        assert !diff.containsKey(PROCESS_ADD);
        assert !diff.containsKey(PROCESS_NOOP);
        assert !diff.containsKey(PROCESS_DELETE);
        assert diff.containsKey(PROCESS_UPDATE);

        assert diff.get(PROCESS_UPDATE).size() == 1;
        assert getFinalList(diff).size() == 1;
    }

    private static void NOOPDup() {
        List<AtlasClassification> currentTags = new ArrayList<>();
        currentTags.add(getTag("alpha", "123", false));

        List<AtlasClassification> newTags = new ArrayList<>();
        newTags.add(getTag("alpha", false));
        newTags.add(getTag("alpha", false));

        List<AtlasClassification> removeTags = new ArrayList<>();

        Map<String, List<AtlasClassification>> diff = AtlasEntityUtils.getTagsDiffForAppend("123", newTags, currentTags, removeTags);

        assert !diff.containsKey(PROCESS_ADD);
        assert diff.containsKey(PROCESS_NOOP);
        assert !diff.containsKey(PROCESS_DELETE);
        assert !diff.containsKey(PROCESS_UPDATE);

        assert diff.get(PROCESS_NOOP).size() == 1;
        assert getFinalList(diff).size() == 1;
    }


    private static AtlasClassification getTag(String name) {
        return getTag(name, null, false);
    }

    private static AtlasClassification getTag(String name, boolean propagate) {
        return getTag(name, null, propagate);
    }

    private static AtlasClassification getTag(String name, String entityGuid) {
        return getTag(name, entityGuid, false);
    }

    private static AtlasClassification getTag(String name, String entityGuid, boolean propagate) {
        AtlasClassification tag = new AtlasClassification();
        tag.setTypeName(name);
        tag.setEntityGuid(entityGuid);
        tag.setPropagate(propagate);

        return tag;
    }

    private static List<AtlasClassification> getFinalList(Map<String, List<AtlasClassification>> diff) {
        List<AtlasClassification> finalTags = new ArrayList<>(0);

        if (diff.containsKey(PROCESS_UPDATE)) {
            finalTags.addAll(diff.get(PROCESS_UPDATE));
        }

        if (diff.containsKey(PROCESS_ADD)) {
            finalTags.addAll(diff.get(PROCESS_ADD));
        }

        if (diff.containsKey(PROCESS_NOOP)) {
            finalTags.addAll(diff.get("NOOP"));
        }

        return finalTags;
    }
}
