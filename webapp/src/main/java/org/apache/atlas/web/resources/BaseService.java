/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.web.resources;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.atlas.catalog.JsonSerializer;
import org.apache.atlas.catalog.Request;
import org.apache.atlas.catalog.ResourceProvider;
import org.apache.atlas.catalog.Result;
import org.apache.atlas.catalog.exception.CatalogException;
import org.apache.atlas.catalog.exception.CatalogRuntimeException;
import org.apache.atlas.catalog.exception.InvalidPayloadException;
import org.apache.atlas.catalog.exception.InvalidQueryException;
import org.apache.atlas.catalog.exception.ResourceNotFoundException;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Collection;
import java.util.Map;

/**
 * Base class for all v1 API services.
 */
public abstract class BaseService {
    private static final Gson gson = new Gson();
    private final Logger LOG = LoggerFactory.getLogger(getClass());
    private final static JsonSerializer serializer = new JsonSerializer();

    protected Result getResource(ResourceProvider provider, Request request)
            throws ResourceNotFoundException {

        try {
            return provider.getResourceById(request);
        } catch (RuntimeException e) {
            throw wrapRuntimeException(e);
        }
    }

    protected Result getResources(ResourceProvider provider, Request request)
            throws ResourceNotFoundException, InvalidQueryException {

        try {
            return provider.getResources(request);
        } catch (RuntimeException e) {
            LOG.error("Error while retrieving taxonomy ", e);
            throw wrapRuntimeException(e);
        }
    }

    protected void createResource(ResourceProvider provider, Request request) throws CatalogException {
        try {
            provider.createResource(request);
        } catch (RuntimeException e) {
            throw wrapRuntimeException(e);
        }
    }

    protected void updateResource(ResourceProvider provider, Request request) throws CatalogException {
        try {
            provider.updateResourceById(request);
        } catch (RuntimeException e) {
            throw wrapRuntimeException(e);
        }
    }

    protected void deleteResource(ResourceProvider provider, Request request) throws CatalogException {
        try {
            provider.deleteResourceById(request);

        } catch (RuntimeException e) {
            throw wrapRuntimeException(e);
        }
    }

    protected Collection<String> createResources(ResourceProvider provider, Request request) throws CatalogException {
        try {
            return provider.createResources(request);
        } catch (RuntimeException e) {
            throw wrapRuntimeException(e);
        }
    }

    protected String getQueryString(@Context UriInfo ui) {
        String uri = ui.getRequestUri().toASCIIString();
        int qsBegin = uri.indexOf("?");
        return (qsBegin == -1) ? null : uri.substring(qsBegin + 1);
    }

    protected <T extends Map> T parsePayload(String body) throws InvalidPayloadException {
        T properties;

        try {
            properties = gson.<T>fromJson(body, Map.class);
        } catch (JsonSyntaxException e) {
            LOG.info("Unable to parse json in request body", e);
            throw new InvalidPayloadException("Request payload contains invalid JSON: " + e.getMessage());
        }

        return properties;
    }

    protected String decode(String s) throws CatalogException {
        try {
            return s == null ? null : URLDecoder.decode(s, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new CatalogException("Unable to decode URL: " + e.getMessage(), 500);
        }
    }

    protected JsonSerializer getSerializer() {
        return serializer;
    }


    private RuntimeException wrapRuntimeException(RuntimeException e) {
        return e instanceof CatalogRuntimeException ? e : new CatalogRuntimeException(e);
    }

    @XmlRootElement
    // the name of this class is used as the collection name in the returned json when returning a collection
    public static class Results {
        public String href;
        public int status;

        public Results() {
            // required by JAXB
        }

        public Results(String href, int status) {
            this.href = href;
            this.status = status;
        }
    }
}
