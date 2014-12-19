
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

package org.apache.hadoop.metadata;

import org.apache.hadoop.metadata.storage.IRepository;
import org.apache.hadoop.metadata.types.TypeSystem;

public class MetadataService {

    final IRepository repo;
    final TypeSystem typeSystem;

    public static final ThreadLocal<MetadataService> currentSvc = new ThreadLocal<MetadataService>();

    public static void setCurrentService(MetadataService svc) {
        currentSvc.set(svc);
    }

    public static MetadataService getCurrentService() throws MetadataException {
        MetadataService m = currentSvc.get();
        if ( m == null ) {
            throw new MetadataException("No MetadataService associated with current thread");
        }
        return m;
    }

    public static IRepository getCurrentRepository() throws MetadataException {
        MetadataService m = currentSvc.get();
        IRepository r = m == null ? null : m.getRepository();
        if ( r == null ) {
            throw new MetadataException("No Repository associated with current thread");
        }
        return r;
    }

    public static TypeSystem getCurrentTypeSystem() throws MetadataException {
        MetadataService m = currentSvc.get();
        TypeSystem t = m == null ? null : m.getTypeSystem();
        if ( t == null ) {
            throw new MetadataException("No TypeSystem associated with current thread");
        }
        return t;
    }

    public MetadataService(IRepository repo, TypeSystem typeSystem) {
        this.typeSystem = typeSystem;
        this.repo = repo;
    }

    public IRepository getRepository() {
        return repo;
    }

    public TypeSystem getTypeSystem() {
        return typeSystem;
    }
}
