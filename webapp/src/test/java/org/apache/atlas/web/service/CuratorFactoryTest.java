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

package org.apache.atlas.web.service;

import com.google.common.base.Charsets;
import org.apache.atlas.AtlasException;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.zookeeper.data.ACL;
import org.mockito.ArgumentMatcher;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class CuratorFactoryTest {
    @Mock
    private Configuration configuration;

    @Mock
    private HAConfiguration.ZookeeperProperties zookeeperProperties;

    @Mock
    private CuratorFrameworkFactory.Builder builder;

    @Mock
    private CuratorFramework curatorFramework;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void shouldAddAuthorization() {
        when(zookeeperProperties.hasAcl()).thenReturn(true);
        when(zookeeperProperties.getAcl()).thenReturn("sasl:myclient@EXAMPLE.COM");
        when(zookeeperProperties.hasAuth()).thenReturn(true);
        when(zookeeperProperties.getAuth()).thenReturn("sasl:myclient@EXAMPLE.COM");

        CuratorFactory curatorFactory = new CuratorFactory(configuration) {
            @Override
            protected void initializeCuratorFramework() {
            }
        };

        curatorFactory.enhanceBuilderWithSecurityParameters(zookeeperProperties, builder);

        verify(builder).aclProvider(any(ACLProvider.class));
        verify(builder).authorization(eq("sasl"), eq("myclient@EXAMPLE.COM".getBytes(Charsets.UTF_8)));
    }

    @Test
    public void shouldAddAclProviderWithRightACL() {
        when(zookeeperProperties.hasAcl()).thenReturn(true);
        when(zookeeperProperties.getAcl()).thenReturn("sasl:myclient@EXAMPLE.COM");
        when(zookeeperProperties.hasAuth()).thenReturn(false);
        CuratorFactory curatorFactory = new CuratorFactory(configuration) {
            @Override
            protected void initializeCuratorFramework() {
            }
        };
        curatorFactory.enhanceBuilderWithSecurityParameters(zookeeperProperties, builder);
        verify(builder).aclProvider(ArgumentMatchers.argThat(new ArgumentMatcher<ACLProvider>() {
            @Override
            public boolean matches(ACLProvider aclProvider) {
                ACL acl = aclProvider.getDefaultAcl().get(0);

                return "myclient@EXAMPLE.COM".equals(acl.getId().getId())
                        && "sasl".equals(acl.getId().getScheme());
            }
        }));
    }

    @Test
    public void shouldNotAddAnySecureParameters() {
        when(zookeeperProperties.hasAcl()).thenReturn(false);
        when(zookeeperProperties.hasAuth()).thenReturn(false);

        CuratorFactory curatorFactory = new CuratorFactory(configuration) {
            @Override
            protected void initializeCuratorFramework() {
            }
        };

        curatorFactory.enhanceBuilderWithSecurityParameters(zookeeperProperties, builder);

        verify(builder, never()).aclProvider(any(ACLProvider.class));
        verify(builder, never()).authorization(anyString(), any(byte[].class));
    }

    @Test
    public void testDefaultConstructor() throws AtlasException {
        CuratorFactory curatorFactory = new CuratorFactory() {
            @Override
            protected void initializeCuratorFramework() {
            }
        };
        assertNotNull(curatorFactory);
    }

    @Test
    public void testClientInstance() {
        CuratorFactory curatorFactory = new CuratorFactory(configuration) {
            @Override
            protected void initializeCuratorFramework() {
                try {
                    Field field = CuratorFactory.class.getDeclaredField("curatorFramework");
                    field.setAccessible(true);
                    field.set(this, curatorFramework);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        CuratorFramework result = curatorFactory.clientInstance();
        assertEquals(result, curatorFramework);
    }

    @Test
    public void testLeaderLatchInstance() {
        CuratorFactory curatorFactory = new CuratorFactory(configuration) {
            @Override
            protected void initializeCuratorFramework() {
                try {
                    Field field = CuratorFactory.class.getDeclaredField("curatorFramework");
                    field.setAccessible(true);
                    field.set(this, curatorFramework);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        String serverId = "server1";
        String zkRoot = "/test";

        LeaderLatch leaderLatch = curatorFactory.leaderLatchInstance(serverId, zkRoot);
        assertNotNull(leaderLatch);
    }

    @Test
    public void testLockInstance() {
        CuratorFactory curatorFactory = new CuratorFactory(configuration) {
            @Override
            protected void initializeCuratorFramework() {
                try {
                    Field field = CuratorFactory.class.getDeclaredField("curatorFramework");
                    field.setAccessible(true);
                    field.set(this, curatorFramework);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        String zkRoot = "/test";

        InterProcessMutex mutex = curatorFactory.lockInstance(zkRoot);
        assertNotNull(mutex);
    }

    @Test
    public void testClose() {
        CuratorFactory curatorFactory = new CuratorFactory(configuration) {
            @Override
            protected void initializeCuratorFramework() {
                try {
                    Field field = CuratorFactory.class.getDeclaredField("curatorFramework");
                    field.setAccessible(true);
                    field.set(this, curatorFramework);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        doNothing().when(curatorFramework).close();
        curatorFactory.close();
        verify(curatorFramework).close();
    }

    @Test
    public void testGetIdForLoggingSaslScheme() throws Exception {
        CuratorFactory curatorFactory = new CuratorFactory(configuration) {
            @Override
            protected void initializeCuratorFramework() {
            }
        };

        Method method = CuratorFactory.class.getDeclaredMethod("getIdForLogging", String.class, String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(curatorFactory, "sasl", "user@EXAMPLE.COM");
        assertEquals(result, "user@EXAMPLE.COM");
    }

    @Test
    public void testGetIdForLoggingIpScheme() throws Exception {
        CuratorFactory curatorFactory = new CuratorFactory(configuration) {
            @Override
            protected void initializeCuratorFramework() {
            }
        };

        Method method = CuratorFactory.class.getDeclaredMethod("getIdForLogging", String.class, String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(curatorFactory, "ip", "192.168.1.1");
        assertEquals(result, "192.168.1.1");
    }

    @Test
    public void testGetIdForLoggingWorldScheme() throws Exception {
        CuratorFactory curatorFactory = new CuratorFactory(configuration) {
            @Override
            protected void initializeCuratorFramework() {
            }
        };

        Method method = CuratorFactory.class.getDeclaredMethod("getIdForLogging", String.class, String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(curatorFactory, "world", "anyone");
        assertEquals(result, "anyone");
    }

    @Test
    public void testGetIdForLoggingAuthScheme() throws Exception {
        CuratorFactory curatorFactory = new CuratorFactory(configuration) {
            @Override
            protected void initializeCuratorFramework() {
            }
        };

        Method method = CuratorFactory.class.getDeclaredMethod("getIdForLogging", String.class, String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(curatorFactory, "auth", "user:password");
        assertEquals(result, "user");
    }

    @Test
    public void testGetIdForLoggingDigestScheme() throws Exception {
        CuratorFactory curatorFactory = new CuratorFactory(configuration) {
            @Override
            protected void initializeCuratorFramework() {
            }
        };

        Method method = CuratorFactory.class.getDeclaredMethod("getIdForLogging", String.class, String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(curatorFactory, "digest", "user:password");
        assertEquals(result, "user");
    }

    @Test
    public void testGetIdForLoggingUnknownScheme() throws Exception {
        CuratorFactory curatorFactory = new CuratorFactory(configuration) {
            @Override
            protected void initializeCuratorFramework() {
            }
        };

        Method method = CuratorFactory.class.getDeclaredMethod("getIdForLogging", String.class, String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(curatorFactory, "unknown", "somevalue");
        assertEquals(result, "unknown");
    }

    @Test
    public void testGetCurrentUser() throws Exception {
        CuratorFactory curatorFactory = new CuratorFactory(configuration) {
            @Override
            protected void initializeCuratorFramework() {
            }
        };

        Method method = CuratorFactory.class.getDeclaredMethod("getCurrentUser");
        method.setAccessible(true);

        String result = (String) method.invoke(curatorFactory);
        assertNotNull(result);
    }

    @Test
    public void testGetAclProviderWithoutAcl() throws Exception {
        CuratorFactory curatorFactory = new CuratorFactory(configuration) {
            @Override
            protected void initializeCuratorFramework() {
            }
        };

        when(zookeeperProperties.hasAcl()).thenReturn(false);

        Method method = CuratorFactory.class.getDeclaredMethod("getAclProvider", HAConfiguration.ZookeeperProperties.class);
        method.setAccessible(true);

        ACLProvider result = (ACLProvider) method.invoke(curatorFactory, zookeeperProperties);
        assertNull(result);
    }

    @Test
    public void testGetBuilderMethod() throws Exception {
        CuratorFactory curatorFactory = new CuratorFactory(configuration) {
            @Override
            protected void initializeCuratorFramework() {
            }
        };

        when(zookeeperProperties.getConnectString()).thenReturn("localhost:2181");
        when(zookeeperProperties.getSessionTimeout()).thenReturn(30000);
        when(zookeeperProperties.getRetriesSleepTimeMillis()).thenReturn(1000);
        when(zookeeperProperties.getNumRetries()).thenReturn(3);

        Method method = CuratorFactory.class.getDeclaredMethod("getBuilder", HAConfiguration.ZookeeperProperties.class);
        method.setAccessible(true);

        CuratorFrameworkFactory.Builder result = (CuratorFrameworkFactory.Builder) method.invoke(curatorFactory, zookeeperProperties);
        assertNotNull(result);
    }

    @Test
    public void testEnhanceBuilderWithSecurityParametersWithAclOnly() {
        when(zookeeperProperties.hasAcl()).thenReturn(true);
        when(zookeeperProperties.getAcl()).thenReturn("digest:user:password");
        when(zookeeperProperties.hasAuth()).thenReturn(false);

        CuratorFactory curatorFactory = new CuratorFactory(configuration) {
            @Override
            protected void initializeCuratorFramework() {
            }
        };

        curatorFactory.enhanceBuilderWithSecurityParameters(zookeeperProperties, builder);

        verify(builder).aclProvider(any(ACLProvider.class));
        verify(builder, never()).authorization(anyString(), any(byte[].class));
    }

    @Test
    public void testEnhanceBuilderWithSecurityParametersWithAuthOnly() {
        when(zookeeperProperties.hasAcl()).thenReturn(false);
        when(zookeeperProperties.hasAuth()).thenReturn(true);
        when(zookeeperProperties.getAuth()).thenReturn("digest:user:password");

        CuratorFactory curatorFactory = new CuratorFactory(configuration) {
            @Override
            protected void initializeCuratorFramework() {
            }
        };

        curatorFactory.enhanceBuilderWithSecurityParameters(zookeeperProperties, builder);

        verify(builder, never()).aclProvider(any(ACLProvider.class));
        verify(builder, never()).authorization(anyString(), any(byte[].class));
    }
}
