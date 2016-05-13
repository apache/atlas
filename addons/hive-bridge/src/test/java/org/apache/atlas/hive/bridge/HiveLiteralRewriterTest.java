/**
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
package org.apache.atlas.hive.bridge;

import org.apache.atlas.hive.hook.HiveHook;
import org.apache.atlas.hive.rewrite.HiveASTRewriter;
import org.apache.atlas.hive.rewrite.RewriteException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class HiveLiteralRewriterTest {

    private HiveConf conf;

    @BeforeClass
    public void setup() {
        conf = new HiveConf();
        conf.addResource("/hive-site.xml");
        SessionState ss = new SessionState(conf, "testuser");
        SessionState.start(ss);
        conf.set("hive.lock.manager", "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");
    }

    @Test
    public void testLiteralRewrite() throws RewriteException {
        HiveHook.HiveEventContext ctx = new HiveHook.HiveEventContext();
        ctx.setQueryStr("insert into table testTable partition(dt='2014-01-01') select * from test1 where dt = '2014-01-01'" +
            " and intColumn = 10" +
            " and decimalColumn = 1.10" +
            " and charColumn = 'a'" +
            " and hexColumn = unhex('\\0xFF')" +
            " and expColumn = cast('-1.5e2' as int)" +
            " and boolCol = true");

            HiveASTRewriter queryRewriter  = new HiveASTRewriter(conf);
            String result = queryRewriter.rewrite(ctx.getQueryStr());
            System.out.println("normlized sql : " + result);

            final String normalizedSQL = "insert into table testTable partition(dt='STRING_LITERAL') " +
                "select * from test1 where dt = 'STRING_LITERAL' " +
                "and intColumn = NUMBER_LITERAL " +
                "and decimalColumn = NUMBER_LITERAL and " +
                "charColumn = 'STRING_LITERAL' and " +
                "hexColumn = unhex('STRING_LITERAL') and " +
                "expColumn = cast('STRING_LITERAL' as int) and " +
                "boolCol = BOOLEAN_LITERAL";
            Assert.assertEquals(result, normalizedSQL);
    }
}
