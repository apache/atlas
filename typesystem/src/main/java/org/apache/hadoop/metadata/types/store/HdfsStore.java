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

package org.apache.hadoop.metadata.types.store;

import com.google.common.collect.ImmutableList;
import com.google.inject.Singleton;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class HdfsStore extends TypeSystemStore {
    public static final String LOCATION_PROPERTY = "metadata.typesystem.store.hdfs.location";

    private static final Path LOCATION = new Path(System.getProperty(LOCATION_PROPERTY));
    private static final Path ARCHIVE_LOCATION = new Path(LOCATION, "ARCHIVE");
    private static final PathFilter PATH_FILTER = new PathFilter() {
        @Override
        public boolean accept(Path path) {
            String name = path.getName();
            return !name.startsWith(".") && !name.startsWith("_") && !name.equals(ARCHIVE_LOCATION.getName());
        }
    };
    private static final String UTF8_ENCODING = "UTF8";
    private static final HdfsStore INSTANCE = new HdfsStore();

    public static HdfsStore getInstance() {
        return INSTANCE;
    }

    @Override
    protected void publish(String namespace, String json) throws StorageException {
        FSDataOutputStream stream = null;
        FileSystem fs = null;
        try {
            fs = LOCATION.getFileSystem(new Configuration());
            Path jsonFile = new Path(LOCATION, namespace + ".json");
            if (fs.exists(jsonFile)) {
                //TODO check if the new json is same and skip update?
                archive(namespace);
            }
            mkdir(fs, jsonFile.getParent());
            stream = fs.create(jsonFile);
            IOUtils.copy(new StringReader(json), stream, UTF8_ENCODING);
        } catch (IOException e) {
            throw new StorageException(namespace, e);
        } finally {
            IOUtils.closeQuietly(stream);
            closeQuietly(fs);
        }
    }

    @Override
    public synchronized  void delete(String namespace) throws StorageException {
        archive(namespace);
    }

    private void archive(String namespace) throws StorageException {
        FileSystem fs = null;
        try {
            fs = LOCATION.getFileSystem(new Configuration());
            Path jsonFile = new Path(LOCATION, namespace + ".json");
            Path archivePath = new Path(ARCHIVE_LOCATION, jsonFile.getName() + System.currentTimeMillis());
            mkdir(fs, archivePath.getParent());
            if (!fs.rename(jsonFile, archivePath)) {
                throw new StorageException(namespace);
            }
        } catch (IOException e) {
            throw new StorageException(namespace, e);
        } finally {
            closeQuietly(fs);
        }
    }

    private void mkdir(FileSystem fs, Path path) throws StorageException {
        try {
            if (!fs.exists(path) && !fs.mkdirs(path)) {
                throw new StorageException("Failed to create " + path);
            }
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    @Override
    protected String fetch(String namespace) throws StorageException {
        FileSystem fs = null;
        FSDataInputStream stream = null;
        try {
            fs = LOCATION.getFileSystem(new Configuration());
            Path file = new Path(LOCATION, namespace + ".json");
            stream = fs.open(file);
            return IOUtils.toString(stream, UTF8_ENCODING);
        } catch (IOException e) {
            throw new StorageException(namespace, e);
        } finally {
            IOUtils.closeQuietly(stream);
            closeQuietly(fs);
        }
    }

    private void closeQuietly(FileSystem fs) throws StorageException {
        if (fs != null) {
            try {
                fs.close();
            } catch (IOException e) {
                throw new StorageException(e);
            }
        }
    }

    @Override
    protected ImmutableList<String> listNamespaces() throws StorageException {
        FileSystem fs = null;
        try {
            fs = LOCATION.getFileSystem(new Configuration());
            FileStatus[] files = fs.listStatus(LOCATION, PATH_FILTER);
            List<String> nameSpaces = new ArrayList<>();
            for (FileStatus file : files) {
                nameSpaces.add(StringUtils.removeEnd(file.getPath().getName(), ".json"));
            }
            return ImmutableList.copyOf(nameSpaces);
        } catch (IOException e) {
            throw new StorageException("list", e);
        } finally {
            closeQuietly(fs);
        }
    }
}
