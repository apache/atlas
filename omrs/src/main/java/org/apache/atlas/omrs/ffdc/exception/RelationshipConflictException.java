/*
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
package org.apache.atlas.omrs.ffdc.exception;

/**
 * RelationshipConflictException provides a checked exception for reporting that a relationship can not be added because
 * it conflicts with a relationship already stored.  The OMRSErrorCode adds specific details for the cause/effect of the error.
 */
public class RelationshipConflictException extends OMRSCheckedExceptionBase
{
    /**
     * This is the typical constructor for creating a RelationshipConflictException.  It captures the essential details
     * about the error, where it occurred and how to fix it.
     *
     * @param httpCode code to use across a REST interface
     * @param className name of class reporting error
     * @param actionDescription description of function it was performing when error detected
     * @param errorMessage description of error
     * @param systemAction actions of the system as a result of the error
     * @param userAction instructions for correcting the error
     */
    public RelationshipConflictException(int httpCode, String className, String  actionDescription, String errorMessage, String systemAction, String userAction)
    {
        super(httpCode, className, actionDescription, errorMessage, systemAction, userAction);
    }


    /**
     * This constructor is used when an unexpected exception has been caught that needs to be wrapped in a
     * RelationshipConflictException in order to add the essential details about the error, where it occurred and
     * how to fix it.
     *
     * @param httpCode code to use across a REST interface
     * @param className name of class reporting error
     * @param actionDescription description of function it was performing when error detected
     * @param errorMessage description of error
     * @param systemAction actions of the system as a result of the error
     * @param userAction instructions for correcting the error
     * @param caughtException the exception/error that caused this exception to be raised
     */
    public RelationshipConflictException(int httpCode, String className, String  actionDescription, String errorMessage, String systemAction, String userAction, Throwable caughtException)
    {
        super(httpCode, className, actionDescription, errorMessage, systemAction, userAction, caughtException);
    }
}
