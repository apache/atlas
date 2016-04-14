/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliRance with
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
package org.apache.atlas.web.security;

import java.util.Properties;

import org.apache.atlas.web.dao.UserDao;
import org.apache.atlas.web.model.User;
import org.junit.Assert;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.testng.annotations.Test;

public class UserDaoTest {

    @Test
    public void testUserDaowithValidUserLoginAndPassword() {

        Properties userLogins = new Properties();
        userLogins.put("admin", "admin123");

        UserDao user = new UserDao();
        user.setUserLogins(userLogins);
        User userBean = user.loadUserByUsername("admin");
        Assert.assertTrue(userBean.getPassword().equals("admin123"));

    }

    @Test
    public void testUserDaowithInValidLogin() {
        boolean hadException = false;
        Properties userLogins = new Properties();
        userLogins.put("admin", "admin123");
        userLogins.put("test", "test123");

        UserDao user = new UserDao();
        user.setUserLogins(userLogins);
        try {
            User userBean = user.loadUserByUsername("xyz");
        } catch (UsernameNotFoundException uex) {
            hadException = true;
        }
        Assert.assertTrue(hadException);
    }

}