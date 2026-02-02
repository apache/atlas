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

package org.apache.atlas.web.errors;

import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Exception mapper for Jersey.
 * @param <E>
 */
@Provider
@Component
public class AllExceptionMapper implements ExceptionMapper<Exception> {

    @Context
    private HttpServletRequest httpServletRequest;

    @Override
    public Response toResponse(Exception exception) {
        final long id = ThreadLocalRandom.current().nextLong();

        // Log request body for bulk endpoints on error (reads from cached request)
        ExceptionMapperUtil.logRequestBodyOnError(httpServletRequest);

        // Log the exception
        ExceptionMapperUtil.logException(exception);
        return Response
                .serverError()
                .entity(ExceptionMapperUtil.formatErrorMessage(id, exception))
                .build();
    }
}
