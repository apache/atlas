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

package org.apache.atlas.web.dao;

import com.google.common.annotations.VisibleForTesting;

import java.io.InputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.List;
import javax.annotation.PostConstruct;
import org.apache.atlas.web.security.AtlasAuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.web.model.User;
import org.apache.commons.configuration.Configuration;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import java.security.MessageDigest;
import org.springframework.security.core.AuthenticationException;
import org.springframework.util.StringUtils;


@Repository
public class UserDao {

    private static final String DEFAULT_USER_CREDENTIALS_PROPERTIES = "users-credentials.properties";

    private static final Logger LOG = LoggerFactory.getLogger(UserDao.class);

    private Properties userLogins;

    @PostConstruct
    public void init() {
        loadFileLoginsDetails();
    }

    void loadFileLoginsDetails() {
        InputStream inStr = null;
        try {
            Configuration configuration = ApplicationProperties.get();
            inStr = ApplicationProperties.getFileAsInputStream(configuration, "atlas.authentication.method.file.filename", DEFAULT_USER_CREDENTIALS_PROPERTIES);
            userLogins = new Properties();
            userLogins.load(inStr);
        } catch (IOException | AtlasException e) {
            LOG.error("Error while reading user.properties file", e);
            throw new RuntimeException(e);
        } finally {
            if(inStr != null) {
                try {
                    inStr.close();
                } catch(Exception excp) {
                    // ignore
                }
            }
        }
    }

    public User loadUserByUsername(final String username)
            throws AuthenticationException {
        String userdetailsStr = userLogins.getProperty(username);
        if (userdetailsStr == null || userdetailsStr.isEmpty()) {
            throw new UsernameNotFoundException("Username not found."
                    + username);
        }
        String password = "";
        String role = "";
        String dataArr[] = userdetailsStr.split("::");
        if (dataArr != null && dataArr.length == 2) {
            role = dataArr[0];
            password = dataArr[1];
        } else {
            LOG.error("User role credentials is not set properly for {}", username);
            throw new AtlasAuthenticationException("User role credentials is not set properly for " + username );
        }

        List<GrantedAuthority> grantedAuths = new ArrayList<>();
        if (StringUtils.hasText(role)) {
            grantedAuths.add(new SimpleGrantedAuthority(role));
        } else {
            LOG.error("User role credentials is not set properly for {}", username);
            throw new AtlasAuthenticationException("User role credentials is not set properly for " + username );
        }

        User userDetails = new User(username, password, grantedAuths);

        return userDetails;
    }
    

    @VisibleForTesting
    public void setUserLogins(Properties userLogins) {
        this.userLogins = userLogins;
    }


    public static String getSha256Hash(String base) throws AtlasAuthenticationException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(base.getBytes("UTF-8"));
            StringBuffer hexString = new StringBuffer();

            for (byte aHash : hash) {
                String hex = Integer.toHexString(0xff & aHash);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }
            return hexString.toString();

        } catch (Exception ex) {
            throw new AtlasAuthenticationException("Exception while encoding password.", ex);
        }
    }

}
