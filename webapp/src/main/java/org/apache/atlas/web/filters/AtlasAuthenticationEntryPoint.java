/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.authentication.LoginUrlAuthenticationEntryPoint;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

@Component
public class AtlasAuthenticationEntryPoint extends LoginUrlAuthenticationEntryPoint {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasAuthenticationEntryPoint.class);

    private static final String LOGIN_PATH = "/login.jsp";

    @Inject
    public AtlasAuthenticationEntryPoint(@Value("/login.jsp") String loginFormUrl) {
        super(loginFormUrl);
    }

    @Override
    public void commence(HttpServletRequest request, HttpServletResponse response, AuthenticationException authException) throws IOException {
        String ajaxRequestHeader = request.getHeader("X-Requested-With");

        response.setHeader("X-Frame-Options", "DENY");

        if ("XMLHttpRequest".equals(ajaxRequestHeader)) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        } else {
            LOG.debug("redirecting to login page loginPath {}", LOGIN_PATH);

            response.sendRedirect(LOGIN_PATH);
        }
    }
}
