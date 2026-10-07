/**
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
package org.apache.atlas.nutch.bridge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;

import static org.testng.Assert.assertEquals;

public class CrawlDbReaderTest {
    @Test
    public void readsPartitionedMapFile() throws Exception {
        File tmp = File.createTempFile("crawldb", "");
        tmp.delete();
        tmp.mkdirs();
        Path current = new Path(new File(tmp, "current").getAbsolutePath());
        Path part = new Path(current, "part-r-00000");
        Configuration conf = new Configuration();

        CrawlDbReader.NutchCrawlDatum value = new CrawlDbReader.NutchCrawlDatum();
        value.status = CrawlRecord.STATUS_DB_FETCHED;
        value.fetchTime = 42L;
        value.score = 2.5f;

        try (MapFile.Writer writer = new MapFile.Writer(conf, part,
                MapFile.Writer.keyClass(Text.class),
                MapFile.Writer.valueClass(CrawlDbReader.NutchCrawlDatum.class),
                MapFile.Writer.compression(SequenceFile.CompressionType.NONE))) {
            writer.append(new Text("https://nutch.apache.org/"), value);
        }

        List<CrawlRecord> records = CrawlDbReader.read(current);

        assertEquals(records.size(), 1);
        assertEquals(records.get(0).getUrl(), "https://nutch.apache.org/");
        assertEquals(records.get(0).getStatus(), CrawlRecord.STATUS_DB_FETCHED);
        assertEquals(records.get(0).getFetchTime(), 42L);
        assertEquals(records.get(0).getScore(), 2.5f, 0.001);
    }
}
