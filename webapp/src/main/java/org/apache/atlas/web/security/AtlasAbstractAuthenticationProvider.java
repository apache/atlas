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

import java.util.ArrayList;
import java.util.List;

import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;

public abstract class AtlasAbstractAuthenticationProvider implements
        AuthenticationProvider {

    @Override
    public boolean supports(Class<?> authentication) {
        return UsernamePasswordAuthenticationToken.class
                .isAssignableFrom(authentication);
    }

    /**
     * 
     * @param authentication
     * @return
     */
    public Authentication getAuthenticationWithGrantedAuthority(
            Authentication authentication) {
        UsernamePasswordAuthenticationToken result = null;
        if (authentication != null && authentication.isAuthenticated()) {
            final List<GrantedAuthority> grantedAuths = getAuthorities(authentication
                    .getName().toString());
            final UserDetails userDetails = new User(authentication.getName()
                    .toString(), authentication.getCredentials().toString(),
                    grantedAuths);
            result = new UsernamePasswordAuthenticationToken(userDetails,
                    authentication.getCredentials(), grantedAuths);
            result.setDetails(authentication.getDetails());
            return result;
        }
        return authentication;
    }

    /**
     * This method will be modified when actual roles are introduced.
     * 
     */
    protected List<GrantedAuthority> getAuthorities(String username) {
        final List<GrantedAuthority> grantedAuths = new ArrayList<GrantedAuthority>();
        grantedAuths.add(new SimpleGrantedAuthority("ROLE_USER"));
        return grantedAuths;
    }

}
