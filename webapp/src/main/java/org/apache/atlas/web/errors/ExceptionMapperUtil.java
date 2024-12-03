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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;

public class ExceptionMapperUtil {
    protected static final Logger LOGGER = LoggerFactory.getLogger(ExceptionMapperUtil.class);

    @SuppressWarnings("UnusedParameters")
    protected static String formatErrorMessage(long id, Exception exception) {
        StringBuilder response = new StringBuilder();

        // Add error ID and general error message
        response.append("{\n")
                .append(String.format("  \"errorId\": \"%016x\",\n", id))
                .append("  \"message\": \"There was an error processing your request.\",\n")
                .append("  \"causes\": [\n");

        // Traverse through the chain of causes, avoiding cycles
        List<String> causes = new ArrayList<>();
        List<Throwable> visited = new ArrayList<>();
        Throwable currentException = exception;
        while (currentException != null) {
            if (visited.contains(currentException)) {
                causes.add("    {\n      \"errorType\": \"CircularReferenceDetected\",\n      \"errorMessage\": \"A circular reference was detected in the exception chain.\",\n      \"location\": \"Unavailable\"\n    }");
                break;
            }
            visited.add(currentException);
            causes.add(formatCause(currentException));
            currentException = currentException.getCause();
        }

        // Add all formatted causes to the response
        for (int i = 0; i < causes.size(); i++) {
            response.append(causes.get(i));
            if (i < causes.size() - 1) {
                response.append(",\n");
            }
        }

        // Close the JSON structure
        response.append("\n  ]\n")
                .append("}");

        return response.toString();
    }

    // Helper method to format a single exception cause
    private static String formatCause(Throwable exception) {
        StringBuilder cause = new StringBuilder();

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

        // Build JSON object for this cause
        cause.append("    {\n")
                .append("      \"errorType\": \"").append(exception.getClass().getName()).append("\",\n")
                .append("      \"errorMessage\": \"").append(exception.getMessage() != null ? exception.getMessage() : "No additional information provided").append("\",\n")
                .append("      \"location\": \"").append(location).append("\"\n")
                .append("    }");

        return cause.toString();
    }



    protected static void logException(long id, Exception exception) {
        LOGGER.error(formatLogMessage(id, exception), exception);
    }

    @SuppressWarnings("UnusedParameters")
    protected static String formatLogMessage(long id, Throwable exception) {
        return String.format("Error handling a request: %016x", id);
    }

}
