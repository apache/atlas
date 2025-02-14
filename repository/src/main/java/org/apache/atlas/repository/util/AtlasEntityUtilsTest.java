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

import org.apache.atlas.RequestContext;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.store.graph.v2.ClassificationAssociator.Updater.PROCESS_ADD;
import static org.apache.atlas.repository.store.graph.v2.ClassificationAssociator.Updater.PROCESS_DELETE;
import static org.apache.atlas.repository.store.graph.v2.ClassificationAssociator.Updater.PROCESS_NOOP;
import static org.apache.atlas.repository.store.graph.v2.ClassificationAssociator.Updater.PROCESS_UPDATE;

public final class AtlasEntityUtilsTest {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasEntityUtilsTest.class);


    public static void main(String[] args) {
        bothEmpty();

        addNewOnly();
        removeAll();
        updateAll();

        noUpdate();

        oneUpdateOneNOOP();
        oneUpdateOneAddOneNOOP();
        oneUpdateOneAddOneRemoveOneNOOP();

        currentOneAddNew();
        addDup();
        updateDup();
        NOOPDup();
    }

    private static void bothEmpty() {
        List<AtlasClassification> currentTags = new ArrayList<>();
        List<AtlasClassification> newTags = new ArrayList<>();

        Map<String, List<AtlasClassification>> diff = AtlasEntityUtils.validateAndGetTagsDiff("123",
                newTags,
                currentTags);
        assert MapUtils.isEmpty(diff);
    }


    private static void addNewOnly() {
        List<AtlasClassification> newTags = new ArrayList<>();
        newTags.add(getTag("alpha"));
        newTags.add(getTag("beta"));

        Map<String, List<AtlasClassification>> diff = AtlasEntityUtils.validateAndGetTagsDiff("123",
                newTags,
                new ArrayList<>());
        assert MapUtils.isNotEmpty(diff);

        assert diff.containsKey(PROCESS_ADD);
        assert !diff.containsKey(PROCESS_DELETE);
        assert !diff.containsKey(PROCESS_UPDATE);
        assert !diff.containsKey(PROCESS_NOOP);

        List<AtlasClassification> resultAdd = diff.get(PROCESS_ADD);
        assert resultAdd.size() == 2;
    }

    private static void removeAll() {
        List<AtlasClassification> currentTags = new ArrayList<>();
        currentTags.add(getTag("alpha"));
        currentTags.add(getTag("beta"));

        List<AtlasClassification> newTags = new ArrayList<>();

        Map<String, List<AtlasClassification>> diff = AtlasEntityUtils.validateAndGetTagsDiff("123", newTags, currentTags);

        assert diff.containsKey(PROCESS_DELETE);
        assert !diff.containsKey(PROCESS_ADD);
        assert !diff.containsKey(PROCESS_UPDATE);
        assert !diff.containsKey(PROCESS_NOOP);

        assert diff.get(PROCESS_DELETE).size() == 2;
    }

    private static void updateAll() {
        List<AtlasClassification> currentTags = new ArrayList<>();
        currentTags.add(getTag("alpha", "123", false));
        currentTags.add(getTag("beta", "123", false));

        List<AtlasClassification> newTags = new ArrayList<>();
        newTags.add(getTag("alpha", true));
        newTags.add(getTag("beta", true));

        Map<String, List<AtlasClassification>> diff = AtlasEntityUtils.validateAndGetTagsDiff("123", newTags, currentTags);

        assert diff.containsKey(PROCESS_UPDATE);
        assert !diff.containsKey(PROCESS_DELETE);
        assert !diff.containsKey(PROCESS_ADD);
        assert !diff.containsKey(PROCESS_NOOP);

        assert diff.get(PROCESS_UPDATE).size() == 2;
    }

    private static void noUpdate() {
        List<AtlasClassification> currentTags = new ArrayList<>();
        currentTags.add(getTag("alpha", "123", true));
        currentTags.add(getTag("beta", "123", true));

        List<AtlasClassification> newTags = new ArrayList<>();
        newTags.add(getTag("alpha", true));
        newTags.add(getTag("beta", true));

        Map<String, List<AtlasClassification>> diff = AtlasEntityUtils.validateAndGetTagsDiff("123", newTags, currentTags);

        assert diff.containsKey(PROCESS_NOOP);
        assert !diff.containsKey(PROCESS_UPDATE);
        assert !diff.containsKey(PROCESS_DELETE);
        assert !diff.containsKey(PROCESS_ADD);

        assert diff.get(PROCESS_NOOP).size() == 2;
    }

    private static void oneUpdateOneNOOP() {
        List<AtlasClassification> currentTags = new ArrayList<>();
        currentTags.add(getTag("alpha", "123", true));
        currentTags.add(getTag("beta", "123", true));

        List<AtlasClassification> newTags = new ArrayList<>();
        newTags.add(getTag("alpha", true));
        newTags.add(getTag("beta", false));

        Map<String, List<AtlasClassification>> diff = AtlasEntityUtils.validateAndGetTagsDiff("123", newTags, currentTags);

        assert diff.containsKey(PROCESS_NOOP);
        assert diff.containsKey(PROCESS_UPDATE);
        assert !diff.containsKey(PROCESS_DELETE);
        assert !diff.containsKey(PROCESS_ADD);

        assert diff.get(PROCESS_NOOP).size() == 1;
        assert diff.get(PROCESS_NOOP).get(0).getTypeName().equals("alpha");

        assert diff.get(PROCESS_UPDATE).size() == 1;
        assert diff.get(PROCESS_UPDATE).get(0).getTypeName().equals("beta");
    }

    private static void oneUpdateOneAddOneNOOP() {
        List<AtlasClassification> currentTags = new ArrayList<>();
        currentTags.add(getTag("alpha", "123", true));
        currentTags.add(getTag("beta", "123", true));

        List<AtlasClassification> newTags = new ArrayList<>();
        newTags.add(getTag("alpha", true));
        newTags.add(getTag("beta", false));
        newTags.add(getTag("gamma", false));

        Map<String, List<AtlasClassification>> diff = AtlasEntityUtils.validateAndGetTagsDiff("123", newTags, currentTags);

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


        Map<String, List<AtlasClassification>> diff = AtlasEntityUtils.validateAndGetTagsDiff("123", newTags, currentTags);

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
    }

    private static void currentOneAddNew() {
        List<AtlasClassification> currentTags = new ArrayList<>();
        currentTags.add(getTag("alpha", "123"));
        currentTags.add(getTag("beta", "123"));

        List<AtlasClassification> newTags = new ArrayList<>();
        newTags.add(getTag("alpha"));
        newTags.add(getTag("beta"));
        newTags.add(getTag("gamma"));

        Map<String, List<AtlasClassification>> diff = AtlasEntityUtils.validateAndGetTagsDiff("123", newTags, currentTags);

        assert diff.containsKey(PROCESS_ADD);
        assert diff.containsKey(PROCESS_NOOP);
        assert !diff.containsKey(PROCESS_DELETE);
        assert !diff.containsKey(PROCESS_UPDATE);

        assert diff.get(PROCESS_ADD).size() == 1;
        assert diff.get(PROCESS_NOOP).size() == 2;
    }

    private static void addDup() {
        List<AtlasClassification> currentTags = new ArrayList<>();

        List<AtlasClassification> newTags = new ArrayList<>();
        newTags.add(getTag("alpha"));
        newTags.add(getTag("alpha"));

        Map<String, List<AtlasClassification>> diff = AtlasEntityUtils.validateAndGetTagsDiff("123", newTags, currentTags);

        assert diff.containsKey(PROCESS_ADD);
        assert !diff.containsKey(PROCESS_NOOP);
        assert !diff.containsKey(PROCESS_DELETE);
        assert !diff.containsKey(PROCESS_UPDATE);

        assert diff.get(PROCESS_ADD).size() == 1;
    }

    private static void updateDup() {
        List<AtlasClassification> currentTags = new ArrayList<>();
        currentTags.add(getTag("alpha", "123", false));

        List<AtlasClassification> newTags = new ArrayList<>();
        newTags.add(getTag("alpha", true));
        newTags.add(getTag("alpha", true));

        Map<String, List<AtlasClassification>> diff = AtlasEntityUtils.validateAndGetTagsDiff("123", newTags, currentTags);

        assert !diff.containsKey(PROCESS_ADD);
        assert !diff.containsKey(PROCESS_NOOP);
        assert !diff.containsKey(PROCESS_DELETE);
        assert diff.containsKey(PROCESS_UPDATE);

        assert diff.get(PROCESS_UPDATE).size() == 1;
    }

    private static void NOOPDup() {
        List<AtlasClassification> currentTags = new ArrayList<>();
        currentTags.add(getTag("alpha", "123", false));

        List<AtlasClassification> newTags = new ArrayList<>();
        newTags.add(getTag("alpha", false));
        newTags.add(getTag("alpha", false));

        Map<String, List<AtlasClassification>> diff = AtlasEntityUtils.validateAndGetTagsDiff("123", newTags, currentTags);

        assert !diff.containsKey(PROCESS_ADD);
        assert diff.containsKey(PROCESS_NOOP);
        assert !diff.containsKey(PROCESS_DELETE);
        assert !diff.containsKey(PROCESS_UPDATE);

        assert diff.get(PROCESS_NOOP).size() == 1;
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
}
