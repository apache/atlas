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

package org.apache.atlas.web.resources;

import org.apache.atlas.catalog.exception.CatalogRuntimeException;
import org.apache.atlas.web.util.Servlets;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Exception mapper for CatalogRuntimeException
 */
@Provider
public class CatalogRuntimeExceptionMapper implements ExceptionMapper<CatalogRuntimeException> {
    @Override
    public Response toResponse(CatalogRuntimeException e) {
        return Response.status(e.getStatusCode()).entity(
                new ErrorBean(e)).type(Servlets.JSON_MEDIA_TYPE).build();
    }

    @XmlRootElement
    public static class ErrorBean {
        private static final String MSG_PREFIX = "An unexpected error has occurred. ";
        public int status;
        public String message;
        public String stackTrace;
        //todo: error code, developerMsg ...

        public ErrorBean() {
            // required for JAXB
        }

        public ErrorBean(CatalogRuntimeException ex) {
            this.status = 500;
            this.message = String.format("%s%s : %s", MSG_PREFIX, ex.toString(), ex.getCause().toString());
            this.stackTrace = getStackTraceFromException(ex);
        }

        public int getStatus() {
            return status;
        }

        public String getMessage() {
            return message;
        }

        public String getStackTrace() {
            return stackTrace;
        }

        private String getStackTraceFromException(RuntimeException e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            return sw.toString();
        }
    }
}
