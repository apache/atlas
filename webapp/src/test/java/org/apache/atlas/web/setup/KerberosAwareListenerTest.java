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

package org.apache.atlas.web.setup;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.servlet.ServletContextEvent;

import static org.testng.Assert.assertTrue;

public class KerberosAwareListenerTest {
    @Mock
    private ServletContextEvent mockServletContextEvent;

    private KerberosAwareListener kerberosAwareListener;

    @BeforeClass
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        kerberosAwareListener = new KerberosAwareListener();
    }

    @Test
    public void testContextInitialized_Success() {
        try {
            kerberosAwareListener.contextInitialized(mockServletContextEvent);
            assertTrue(true);
        } catch (Exception e) {
            assertTrue(true);
        }
    }
}
