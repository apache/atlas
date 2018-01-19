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
package org.apache.atlas.ocf.ffdc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ConnectorCheckedException provides a checked exception for reporting errors found when using OCF connectors.
 * Typically these errors are either configuration or operational errors that can be fixed by an administrator
 * or power user.  However, there may be the odd bug that surfaces here. The OCFErrorCode can be used with
 * this exception to populate it with standard messages.  Otherwise messages defined uniquely for a
 * ConnectorProvider/Connector implementation can be used.  The aim is to be able to uniquely identify the cause
 * and remedy for the error.
 */
public class ConnectorCheckedException extends OCFCheckedExceptionBase
{
    private static final Logger log = LoggerFactory.getLogger(ConnectorCheckedException.class);

    /**
     * This is the typical constructor used for creating a ConnectorCheckedException.
     *
     * @param httpCode http response code to use if this exception flows over a REST call
     * @param className name of class reporting error
     * @param actionDescription description of function it was performing when error detected
     * @param errorMessage description of error
     * @param systemAction actions of the system as a result of the error
     * @param userAction instructions for correcting the error
     */
    public ConnectorCheckedException(int  httpCode, String className, String  actionDescription, String errorMessage, String systemAction, String userAction)
    {
        super(httpCode, className, actionDescription, errorMessage, systemAction, userAction);

        if (log.isDebugEnabled())
        {
            log.debug(httpCode + ", " + className + ", " + actionDescription);
        }
    }


    /**
     * This is the constructor used for creating a ConnectorCheckedException in response to a previous exception.
     *
     * @param httpCode http response code to use if this exception flows over a REST call
     * @param className name of class reporting error
     * @param actionDescription description of function it was performing when error detected
     * @param errorMessage - description of error
     * @param systemAction - actions of the system as a result of the error
     * @param userAction - instructions for correcting the error
     * @param caughtError - the error that resulted in this exception.
     */
    public ConnectorCheckedException(int  httpCode, String className, String  actionDescription, String errorMessage, String systemAction, String userAction, Throwable caughtError)
    {
        super(httpCode, className, actionDescription, errorMessage, systemAction, userAction, caughtError);

        if (log.isDebugEnabled())
        {
            log.debug(httpCode + ", " + className + ", " + actionDescription + ", " + caughtError.toString());
        }
    }
}