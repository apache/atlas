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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads Nutch HostDB MapFile partitions and SequenceFile {@code part-*} files
 * without depending on Nutch jars. Value layout matches
 * {@code org.apache.nutch.hostdb.HostDatum}.
 */
public final class HostDbReader {
    private HostDbReader() {
    }

    /**
     * Reads HostDB MapFile partitions and SequenceFile {@code part-*} files.
     *
     * @param hostDbCurrent {@code hostdb/current} or a HostDB partition path
     * @return host rows with fetched / unfetched counters
     * @throws IOException if no HostDB files are found or a partition cannot be read
     */
    public static List<HostDbRecord> read(Path hostDbCurrent) throws IOException {
        Configuration conf = new Configuration();
        FileSystem    fs   = hostDbCurrent.getFileSystem(conf);
        Path          dir  = hostDbCurrent;

        if (fs.isFile(dir)) {
            dir = dir.getParent();
        }

        List<HostDbRecord> records = new ArrayList<>();
        List<Path>         mapFiles = new ArrayList<>();
        List<Path>         sequenceFiles = new ArrayList<>();

        if (fs.exists(new Path(dir, "data"))) {
            mapFiles.add(dir);
        } else {
            for (FileStatus status : fs.listStatus(dir)) {
                if (status.isDirectory() && fs.exists(new Path(status.getPath(), "data"))) {
                    mapFiles.add(status.getPath());
                } else if (status.isFile() && status.getPath().getName().startsWith("part-")) {
                    sequenceFiles.add(status.getPath());
                }
            }
        }

        if (mapFiles.isEmpty() && sequenceFiles.isEmpty()) {
            throw new IOException("No readable HostDB files found under " + dir);
        }

        for (Path mapFile : mapFiles) {
            try (MapFile.Reader reader = new MapFile.Reader(mapFile, conf)) {
                Text           key   = new Text();
                NutchHostDatum value = (NutchHostDatum) ReflectionUtils.newInstance(NutchHostDatum.class, conf);

                while (reader.next(key, value)) {
                    records.add(toRecord(key, value));
                }
            }
        }

        for (Path sequenceFile : sequenceFiles) {
            try (SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(sequenceFile))) {
                Text           key   = new Text();
                NutchHostDatum value = (NutchHostDatum) ReflectionUtils.newInstance(NutchHostDatum.class, conf);

                while (reader.next(key, value)) {
                    records.add(toRecord(key, value));
                }
            }
        }

        return records;
    }

    private static HostDbRecord toRecord(Text key, NutchHostDatum value) {
        return new HostDbRecord(key.toString(), value.score, value.lastCheck, value.homepageUrl,
                value.dnsFailures, value.connectionFailures, value.unfetched, value.fetched,
                value.notModified, value.redirTemp, value.redirPerm, value.gone);
    }

    /**
     * One HostDB hostname row. Layout matches {@code org.apache.nutch.hostdb.HostDatum}.
     */
    public static class HostDbRecord {
        final String hostname;
        final float  score;
        final long   lastCheck;
        final String homepageUrl;
        final long   dnsFailures;
        final long   connectionFailures;
        final long   unfetched;
        final long   fetched;
        final long   notModified;
        final long   redirTemp;
        final long   redirPerm;
        final long   gone;

        /**
         * @param hostname           HostDB key
         * @param score              host score
         * @param lastCheck          last check time in milliseconds since epoch
         * @param homepageUrl        optional homepage URL
         * @param dnsFailures        DNS failure count
         * @param connectionFailures connection failure count
         * @param unfetched          unfetched URL count
         * @param fetched            fetched URL count
         * @param notModified        not-modified URL count
         * @param redirTemp          temporary redirect count
         * @param redirPerm          permanent redirect count
         * @param gone               gone URL count
         */
        public HostDbRecord(String hostname, float score, long lastCheck, String homepageUrl, long dnsFailures,
                long connectionFailures, long unfetched, long fetched, long notModified, long redirTemp,
                long redirPerm, long gone) {
            this.hostname = hostname;
            this.score = score;
            this.lastCheck = lastCheck;
            this.homepageUrl = homepageUrl;
            this.dnsFailures = dnsFailures;
            this.connectionFailures = connectionFailures;
            this.unfetched = unfetched;
            this.fetched = fetched;
            this.notModified = notModified;
            this.redirTemp = redirTemp;
            this.redirPerm = redirPerm;
            this.gone = gone;
        }
    }

    /**
     * Writable subset of Nutch {@code HostDatum} used to deserialize HostDB values.
     */
    public static class NutchHostDatum implements Writable {
        float  score;
        long   lastCheck;
        String homepageUrl = "";
        long   dnsFailures;
        long   connectionFailures;
        long   unfetched;
        long   fetched;
        long   notModified;
        long   redirTemp;
        long   redirPerm;
        long   gone;

        /** {@inheritDoc} */
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeFloat(score);
            out.writeLong(lastCheck);
            Text.writeString(out, homepageUrl == null ? "" : homepageUrl);
            out.writeLong(dnsFailures);
            out.writeLong(connectionFailures);
            out.writeLong(unfetched);
            out.writeLong(fetched);
            out.writeLong(notModified);
            out.writeLong(redirTemp);
            out.writeLong(redirPerm);
            out.writeLong(gone);
            new MapWritable().write(out);
        }

        /** {@inheritDoc} */
        @Override
        public void readFields(DataInput in) throws IOException {
            score = in.readFloat();
            lastCheck = in.readLong();
            homepageUrl = Text.readString(in);
            dnsFailures = in.readLong();
            connectionFailures = in.readLong();
            unfetched = in.readLong();
            fetched = in.readLong();
            notModified = in.readLong();
            redirTemp = in.readLong();
            redirPerm = in.readLong();
            gone = in.readLong();
            MapWritable meta = new MapWritable();
            meta.readFields(in);
        }
    }
}
