/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.atlas.web.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.web.filters.HeadersUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.authentication.logout.LogoutSuccessHandler;
import org.springframework.security.web.authentication.logout.SimpleUrlLogoutSuccessHandler;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Component
public class CustomLogoutSuccessHandler extends SimpleUrlLogoutSuccessHandler implements LogoutSuccessHandler {
    private final ObjectMapper mapper;
    private static final Logger LOG = LoggerFactory.getLogger(CustomLogoutSuccessHandler.class);

    @Inject
    public CustomLogoutSuccessHandler(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public void onLogoutSuccess(HttpServletRequest request, HttpServletResponse response, Authentication authentication) {
        request.getServletContext().removeAttribute(request.getRequestedSessionId());
        response.setContentType("application/json;charset=UTF-8");
        response.setHeader(HeadersUtil.CACHE_CONTROL, HeadersUtil.CACHE_CONTROL_VAL);
        response.setHeader(HeadersUtil.X_FRAME_OPTIONS_KEY, HeadersUtil.X_FRAME_OPTIONS_VAL);

        try {
            Map<String, Object> responseMap = new HashMap<>();
            responseMap.put("statusCode", HttpServletResponse.SC_OK);
            responseMap.put("msgDesc", "Logout Successful");
            String jsonStr = mapper.writeValueAsString(responseMap);

            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(jsonStr);

            LOG.debug("Log-out Successfully done. Returning Json : {}", jsonStr);
        } catch (IOException e) {
            LOG.debug("Error while writing JSON in HttpServletResponse");
        }
    }
}
