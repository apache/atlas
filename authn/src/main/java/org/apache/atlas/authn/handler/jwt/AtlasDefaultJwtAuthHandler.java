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
package org.apache.atlas.authn.handler.jwt;

import com.nimbusds.jose.proc.JWSKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import com.nimbusds.jwt.proc.JWTClaimsSetVerifier;
import org.apache.atlas.authn.handler.AtlasAuth;
import org.apache.commons.lang3.StringUtils;

import javax.servlet.ServletRequest;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;

public class AtlasDefaultJwtAuthHandler extends AtlasJwtAuthHandler {
    protected static final String AUTHORIZATION_HEADER = "Authorization";

    @Override
    public ConfigurableJWTProcessor<SecurityContext> getJwtProcessor(JWSKeySelector<SecurityContext> keySelector) {
        ConfigurableJWTProcessor<SecurityContext> jwtProcessor = new DefaultJWTProcessor<>();
        JWTClaimsSetVerifier<SecurityContext> claimsVerifier   = new DefaultJWTClaimsVerifier<>();

        jwtProcessor.setJWSKeySelector(keySelector);
        jwtProcessor.setJWTClaimsSetVerifier(claimsVerifier);

        return jwtProcessor;
    }

    @Override
    public AtlasAuth authenticate(HttpServletRequest request) {
        AtlasAuth atlasAuth = null;
        String jwtAuthHeaderStr = getJwtAuthHeader(request);
        String jwtCookieStr     = StringUtils.isBlank(jwtAuthHeaderStr) ? getJwtCookie(request) : null;

        String username = authenticate(jwtAuthHeaderStr, jwtCookieStr);
        if (username != null) {
            atlasAuth = new AtlasAuth(username, AtlasAuth.AuthType.JWT_JWKS);
        }
        return atlasAuth;
    }

    public static boolean canAuthenticateRequest(final ServletRequest request) {
        HttpServletRequest httpServletRequest = (HttpServletRequest) request;
        String jwtAuthHeaderStr               = getJwtAuthHeader(httpServletRequest);
        String jwtCookieStr                   = StringUtils.isBlank(jwtAuthHeaderStr) ? getJwtCookie(httpServletRequest) : null;
        return shouldProceedAuth(jwtAuthHeaderStr, jwtCookieStr);
    }

    public static String getJwtAuthHeader(final HttpServletRequest httpServletRequest) {
        return httpServletRequest.getHeader(AUTHORIZATION_HEADER);
    }

    public static String getJwtCookie(final HttpServletRequest httpServletRequest) {
        String jwtCookieStr = null;
        Cookie[] cookies    = httpServletRequest.getCookies();

        if (cookies != null) {
            for (Cookie cookie : cookies) {
                if (cookieName.equals(cookie.getName())) {
                    jwtCookieStr = cookie.getName() + "=" + cookie.getValue();
                    break;
                }
            }
        }
        return jwtCookieStr;
    }
}
