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
package org.apache.atlas;

import org.apache.atlas.graph.GraphSandboxUtil;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.testng.ITestContext;
import org.testng.TestListenerAdapter;
import org.testng.xml.XmlClass;

import java.util.List;

public class DBSandboxer extends TestListenerAdapter {
    @Override
    public void onStart(ITestContext context) {
        // This will only work if each test is run individually (test suite has only one running test)
        // If there are multiple tests the the sandbox folder name is not provided and the GraphSandboxUtil provisions
        // a unique name
        List<XmlClass> testClassesToRun = context.getCurrentXmlTest().getClasses();
        if (CollectionUtils.isNotEmpty(testClassesToRun) && 1 == testClassesToRun.size()) {
            XmlClass currentTestClass = testClassesToRun.get(0);
            if (null != currentTestClass && StringUtils.isNotEmpty(currentTestClass.getName())) {
                GraphSandboxUtil.create(currentTestClass.getName());
            } else {
                GraphSandboxUtil.create();
            }
        } else {
            GraphSandboxUtil.create();
        }
    }

    @Override
    public void onFinish(ITestContext context) {
        AtlasGraphProvider.cleanup();
    }
}
