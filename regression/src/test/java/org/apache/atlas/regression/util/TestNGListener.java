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
package org.apache.atlas.regression.util;

import org.apache.log4j.Logger;
import org.apache.log4j.NDC;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;

public class TestNGListener implements ITestListener {
    private static final Logger LOGGER = Logger.getLogger(TestNGListener.class);
    private final String header = StringUtils.repeat("-", 80);
    private final String footer = StringUtils.repeat("#", 80);
    private enum resultStatus {SUCCESS, FAILED, SKIPPED, TestFailedButWithinSuccessPercentage }

    public void onTestStart(ITestResult iTestResult) {
        LOGGER.info(header);
        LOGGER.info(
                String.format("Testing going to start for: %s.%s(%s)", iTestResult.getTestClass().getName(),
                        iTestResult.getName(), Arrays.toString(iTestResult.getParameters())));
        NDC.push(iTestResult.getName());
    }

    public void onTestSuccess(ITestResult iTestResult) {
        endOfTestHook(iTestResult, resultStatus.SUCCESS);
    }

    public void onTestFailure(ITestResult iTestResult) {
        endOfTestHook(iTestResult, resultStatus.FAILED);

    }

    public void onTestSkipped(ITestResult iTestResult) {
        endOfTestHook(iTestResult, resultStatus.SKIPPED);

    }

    public void onTestFailedButWithinSuccessPercentage(ITestResult iTestResult) {
        endOfTestHook(iTestResult, resultStatus.TestFailedButWithinSuccessPercentage);

    }

    public void onStart(ITestContext iTestContext) {

    }

    public void onFinish(ITestContext iTestContext) {

    }

    private void endOfTestHook(ITestResult result, resultStatus outcome) {
        LOGGER.info(
                String.format("Testing going to end for: %s.%s(%s) %s", result.getTestClass().getName(),
                        result.getName(), Arrays.toString(result.getParameters()), outcome));
        LOGGER.info(footer);
        NDC.pop();
    }
}
