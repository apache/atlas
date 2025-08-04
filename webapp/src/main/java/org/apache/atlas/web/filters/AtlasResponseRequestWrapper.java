/*
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
package org.apache.atlas.web.filters;

import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;


public class AtlasResponseRequestWrapper extends HttpServletResponseWrapper {
    private final long startTime;
    private boolean timingHeaderSet = false;
    private static final String RESPONSE_TIME_HEADER = "X-Atlan-Api-Response-Time";
    
    public AtlasResponseRequestWrapper(HttpServletResponse response) {
        super(response);
        this.startTime = System.currentTimeMillis();
    }
    
    public AtlasResponseRequestWrapper(HttpServletResponse response, long startTime) {
        super(response);
        this.startTime = startTime;
    }
    
    @Override
    public void flushBuffer() throws java.io.IOException {
        setTimingHeaderIfNeeded();
        super.flushBuffer();
    }
    
    @Override
    public void setContentLength(int len) {
        setTimingHeaderIfNeeded();
        super.setContentLength(len);
    }
    
    private void setTimingHeaderIfNeeded() {
        if (!timingHeaderSet && !isCommitted()) {
            long responseTime = System.currentTimeMillis() - startTime;
            setHeader(RESPONSE_TIME_HEADER, String.valueOf(responseTime));
            timingHeaderSet = true;
        }
    }
}





