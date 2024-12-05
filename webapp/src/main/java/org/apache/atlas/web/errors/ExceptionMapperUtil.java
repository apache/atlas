/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.web.errors;

import org.apache.atlas.type.AtlasType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class ExceptionMapperUtil {
    protected static final Logger LOGGER = LoggerFactory.getLogger(ExceptionMapperUtil.class);

    @SuppressWarnings("UnusedParameters")
    protected static String formatErrorMessage(long id, Exception exception) {
        if (exception == null) {
            // If the exception is null, return a minimal error message
            Map<String, Object> errorDetails = new HashMap<>();
            errorDetails.put("errorId", String.format("%016x", id));
            errorDetails.put("message", "No exception provided.");
            errorDetails.put("causes", new ArrayList<>());
            return AtlasType.toJson(errorDetails);
        }

        // Prepare data for error message
        Map<String, Object> errorDetails = new HashMap<>();
        errorDetails.put("errorId", String.format("%016x", id));
        errorDetails.put("message", "There was an error processing your request.");

        // Create a list of causes
        List<Map<String, String>> causes = new ArrayList<>();
        List<Throwable> visited = new ArrayList<>();
        Throwable currentException = exception;

        while (currentException != null) {
            if (visited.contains(currentException)) {
                // If circular reference detected, add special entry
                Map<String, String> circularCause = new HashMap<>();
                circularCause.put("errorType", "CircularReferenceDetected");
                circularCause.put("errorMessage", "A circular reference was detected in the exception chain.");
                circularCause.put("location", "Unavailable");
                causes.add(circularCause);
                break;
            }
            visited.add(currentException);
            causes.add(formatCause(currentException));
            currentException = currentException.getCause();
        }

        errorDetails.put("causes", causes);

        return AtlasType.toJson(errorDetails);
    }

    // Helper method to format a single exception cause
    private static Map<String, String> formatCause(Throwable exception) {
        Map<String, String> cause = new HashMap<>();

        // Extract location details from the first stack trace element
        StackTraceElement[] stackTrace = exception.getStackTrace();
        String location = "Unavailable";
        if (stackTrace != null && stackTrace.length > 0) {
            StackTraceElement element = stackTrace[0];
            location = String.format("%s.%s (%s:%d)",
                    element.getClassName(),
                    element.getMethodName(),
                    element.getFileName(),
                    element.getLineNumber());
        }

        // Populate the cause map
        cause.put("errorType", exception.getClass().getName());
        cause.put("errorMessage", exception.getMessage() != null ? exception.getMessage() : "No additional information provided");
        cause.put("location", location);

        return cause;
    }

    protected static void logException(long id, Exception exception) {
        LOGGER.error(formatLogMessage(id, exception), exception);
    }

    @SuppressWarnings("UnusedParameters")
    protected static String formatLogMessage(long id, Throwable exception) {
        return String.format("Error handling a request: %016x , exception is: %s", id, exception.getMessage());
    }
}
