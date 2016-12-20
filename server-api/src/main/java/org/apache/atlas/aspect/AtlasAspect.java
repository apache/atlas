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

package org.apache.atlas.aspect;

import org.apache.atlas.RequestContext;
import org.apache.atlas.metrics.Metrics;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

@Aspect
public class AtlasAspect {

    public static final Logger LOG = LoggerFactory.getLogger(AtlasAspect.class);

    @Around("@annotation(org.apache.atlas.aspect.Monitored) && execution(* *(..))")
    public Object collectMetricsForMonitored(ProceedingJoinPoint joinPoint) throws Throwable {
        Signature methodSign = joinPoint.getSignature();
        Metrics metrics = RequestContext.getMetrics();
        String metricName = methodSign.getDeclaringType().getSimpleName() + "." + methodSign.getName();
        long start = System.currentTimeMillis();

        try {
            Object response = joinPoint.proceed();
            return response;
        } finally {
            metrics.record(metricName, (System.currentTimeMillis() - start));
        }
    }

    @Around("@annotation(org.apache.atlas.aspect.Loggable) && execution(* *(..))")
    public Object logAroundLoggable(ProceedingJoinPoint joinPoint) throws Throwable {
        Signature methodSign = joinPoint.getSignature();
        String methodName = methodSign.getDeclaringType().getSimpleName() + "." + methodSign.getName();

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("==> %s(%s)", methodName, Arrays.toString(joinPoint.getArgs())));
        }
        Object response = joinPoint.proceed();
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("<== %s(%s): %s", methodName, Arrays.toString(joinPoint.getArgs()),
                    response instanceof List ? ((List)response).size() : response));
        }
        return response;
    }
}