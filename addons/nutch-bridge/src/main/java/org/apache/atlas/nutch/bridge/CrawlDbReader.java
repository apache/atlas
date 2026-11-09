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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads Nutch CrawlDb MapFiles ({@code crawldb/current}) without depending on Nutch jars.
 * Value layout matches Nutch CrawlDatum version 7.
 */
public final class CrawlDbReader {
    private CrawlDbReader() {
    }

    /**
     * Reads every MapFile partition under {@code crawldb/current}.
     *
     * @param crawlDbCurrent {@code crawldb/current} or a single MapFile directory
     * @return one record per CrawlDb URL
     * @throws IOException if no MapFiles are found or a partition cannot be read
     */
    public static List<CrawlRecord> read(Path crawlDbCurrent) throws IOException {
        Configuration conf = new Configuration();
        FileSystem    fs   = crawlDbCurrent.getFileSystem(conf);
        Path          dir  = crawlDbCurrent;

        if (fs.isFile(dir)) {
            dir = dir.getParent();
        }

        List<CrawlRecord> records = new ArrayList<>();
        List<Path>        mapFiles = new ArrayList<>();

        if (fs.exists(new Path(dir, "data"))) {
            mapFiles.add(dir);
        } else {
            for (FileStatus status : fs.listStatus(dir)) {
                if (status.isDirectory() && fs.exists(new Path(status.getPath(), "data"))) {
                    mapFiles.add(status.getPath());
                }
            }
        }

        if (mapFiles.isEmpty()) {
            throw new IOException("No readable CrawlDb MapFiles found under " + dir);
        }

        for (Path mapFile : mapFiles) {
            try (MapFile.Reader reader = new MapFile.Reader(mapFile, conf)) {
                Text            key   = new Text();
                NutchCrawlDatum value = (NutchCrawlDatum) ReflectionUtils.newInstance(NutchCrawlDatum.class, conf);

                while (reader.next(key, value)) {
                    records.add(new CrawlRecord(key.toString(), value.status, value.score, value.fetchTime));
                }
            }
        }

        return records;
    }

    /**
     * Subset of Nutch CrawlDatum Writable for v7.
     */
    public static class NutchCrawlDatum implements Writable {
        byte  status;
        long  fetchTime;
        byte  retries;
        int   fetchInterval;
        float score;
        long  modifiedTime;

        /** {@inheritDoc} */
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeByte(7);
            out.writeByte(status);
            out.writeLong(fetchTime);
            out.writeByte(retries);
            out.writeInt(fetchInterval);
            out.writeFloat(score);
            out.writeLong(modifiedTime);
            out.writeByte(0);
            out.writeBoolean(false);
        }

        /** {@inheritDoc} */
        @Override
        public void readFields(DataInput in) throws IOException {
            byte version = in.readByte();

            status    = in.readByte();
            fetchTime = in.readLong();
            retries   = in.readByte();

            if (version > 5) {
                fetchInterval = in.readInt();
            } else {
                fetchInterval = Math.round(in.readFloat());
            }

            score = in.readFloat();

            if (version > 2) {
                modifiedTime = in.readLong();
                int sigLen = in.readByte() & 0xff;
                if (sigLen > 0) {
                    in.skipBytes(sigLen);
                }
            }

            if (version > 3) {
                boolean hasMetadata = in.readBoolean();
                if (hasMetadata) {
                    if (!(in instanceof DataInputStream)) {
                        throw new IOException("Cannot skip CrawlDatum metadata from " + in.getClass().getName());
                    }

                    in.skipBytes(((DataInputStream) in).available());
                }
            }
        }
    }
}
