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
 * See the License for the specific language governing limitations under
 * the License.
 */
package org.apache.atlas.nutch.bridge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;

import static org.testng.Assert.assertEquals;

public class HostDbReaderTest {
    @Test
    public void writableRoundTripMatchesHostDatumLayout() throws Exception {
        HostDbReader.NutchHostDatum written = new HostDbReader.NutchHostDatum();
        written.score = 1.5f;
        written.lastCheck = 42L;
        written.homepageUrl = "https://example.com/";
        written.dnsFailures = 1;
        written.connectionFailures = 2;
        written.unfetched = 3;
        written.fetched = 4;
        written.notModified = 5;
        written.redirTemp = 6;
        written.redirPerm = 7;
        written.gone = 8;

        DataOutputBuffer out = new DataOutputBuffer();
        written.write(out);

        HostDbReader.NutchHostDatum read = new HostDbReader.NutchHostDatum();
        DataInputBuffer in = new DataInputBuffer();
        in.reset(out.getData(), 0, out.getLength());
        read.readFields(in);

        assertEquals(read.score, 1.5f, 0.001);
        assertEquals(read.lastCheck, 42L);
        assertEquals(read.homepageUrl, "https://example.com/");
        assertEquals(read.dnsFailures, 1);
        assertEquals(read.fetched, 4);
        assertEquals(read.notModified, 5);
        assertEquals(read.gone, 8);
    }

    @Test
    public void mapFileRoundTrip() throws Exception {
        File tmp = File.createTempFile("hostdb", "");
        tmp.delete();
        tmp.mkdirs();
        Path current = new Path(new File(tmp, "current").getAbsolutePath());
        Path dir = new Path(current, "part-r-00000");
        Configuration conf = new Configuration();

        HostDbReader.NutchHostDatum value = new HostDbReader.NutchHostDatum();
        value.fetched = 2;
        value.notModified = 1;
        value.score = 9.0f;
        value.lastCheck = 99L;

        try (MapFile.Writer writer = new MapFile.Writer(conf, dir,
                MapFile.Writer.keyClass(Text.class),
                MapFile.Writer.valueClass(HostDbReader.NutchHostDatum.class),
                MapFile.Writer.compression(SequenceFile.CompressionType.NONE))) {
            writer.append(new Text("lucene.apache.org"), value);
        }

        List<HostDbReader.HostDbRecord> records = HostDbReader.read(current);
        assertEquals(records.size(), 1);
        assertEquals(records.get(0).hostname, "lucene.apache.org");
        assertEquals(records.get(0).fetched, 2);
        assertEquals(records.get(0).notModified, 1);
        assertEquals(records.get(0).score, 9.0f, 0.001);
        assertEquals(records.get(0).lastCheck, 99L);
    }

    @Test
    public void sequenceFileRoundTrip() throws Exception {
        File tmp = File.createTempFile("hostdb", "");
        tmp.delete();
        tmp.mkdirs();
        Path current = new Path(new File(tmp, "current").getAbsolutePath());
        Path part = new Path(current, "part-r-00000");
        Configuration conf = new Configuration();

        HostDbReader.NutchHostDatum value = new HostDbReader.NutchHostDatum();
        value.fetched = 1;
        value.score = 3.0f;

        try (SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(part),
                SequenceFile.Writer.keyClass(Text.class),
                SequenceFile.Writer.valueClass(HostDbReader.NutchHostDatum.class))) {
            writer.append(new Text("nutch.apache.org"), value);
        }

        List<HostDbReader.HostDbRecord> records = HostDbReader.read(current);

        assertEquals(records.size(), 1);
        assertEquals(records.get(0).hostname, "nutch.apache.org");
        assertEquals(records.get(0).fetched, 1);
        assertEquals(records.get(0).score, 3.0f, 0.001);
    }
}
