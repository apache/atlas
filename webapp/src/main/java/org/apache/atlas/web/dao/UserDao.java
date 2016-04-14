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
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import javax.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.web.model.User;
import org.apache.commons.configuration.Configuration;
import org.springframework.security.core.userdetails.UsernameNotFoundException;

@Repository
public class UserDao {

    private static final Logger LOG = LoggerFactory.getLogger(UserDao.class);

    private Properties userLogins;

    @PostConstruct
    public void init() {
        loadFileLoginsDetails();
    }

    void loadFileLoginsDetails() {
        String PROPERTY_FILE_PATH = null;
        try {

            Configuration configuration = ApplicationProperties.get();
            PROPERTY_FILE_PATH = configuration
                    .getString("atlas.login.credentials.file");
            if (PROPERTY_FILE_PATH != null && !"".equals(PROPERTY_FILE_PATH)) {
                userLogins = new Properties();
                userLogins.load(new FileInputStream(PROPERTY_FILE_PATH));
            }else {
                LOG.error("Error while reading user.properties file, filepath="
                        + PROPERTY_FILE_PATH);
            }

        } catch (IOException | AtlasException e) {
            LOG.error("Error while reading user.properties file, filepath="
                    + PROPERTY_FILE_PATH, e);
        }
    }

    public User loadUserByUsername(final String username)
            throws UsernameNotFoundException {
        String password = userLogins.getProperty(username);
        if (password == null || password.isEmpty()) {
            throw new UsernameNotFoundException("Username not found."
                    + username);
        }
        User user = new User();
        user.setUsername(username);
        user.setPassword(password);
        return user;
    }

    @VisibleForTesting
    public void setUserLogins(Properties userLogins) {
        this.userLogins = userLogins;
    }

}
