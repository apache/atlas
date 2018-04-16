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
package org.apache.atlas.omas.assetconsumer.ffdc.exceptions;

/**
 * AssetConsumerCheckedExceptionBase provides a checked exception for reporting errors found when using
 * the Asset Consumer OMAS services.
 *
 * Typically these errors are either configuration or operational errors that can be fixed by an administrator
 * or power AssetConsumerInterface.  However, there may be the odd bug that surfaces here. The AssetConsumerErrorCode can be used with
 * this exception to populate it with standard messages.  The aim is to be able to uniquely identify the cause
 * and remedy for the error.
 */
public class AssetConsumerCheckedExceptionBase extends Exception
{
    /*
     * These default values are only seen if this exception is initialized using one of its superclass constructors.
     */
    private int       reportedHTTPCode;
    private String    reportingClassName;
    private String    reportingActionDescription;
    private String    reportedErrorMessage;
    private String    reportedSystemAction;
    private String    reportedUserAction;
    private Throwable reportedCaughtException = null;


    /**
     * This is the typical constructor used for creating a ConnectionCheckedException.
     *
     * @param httpCode - http response code to use if this exception flows over a rest call
     * @param className - name of class reporting error
     * @param actionDescription - description of function it was performing when error detected
     * @param errorMessage - description of error
     * @param systemAction - actions of the system as a result of the error
     * @param userAction - instructions for correcting the error
     */
    public AssetConsumerCheckedExceptionBase(int    httpCode,
                                             String className,
                                             String actionDescription,
                                             String errorMessage,
                                             String systemAction,
                                             String userAction)
    {
        super(errorMessage);
        this.reportedHTTPCode = httpCode;
        this.reportingClassName = className;
        this.reportingActionDescription = actionDescription;
        this.reportedErrorMessage = errorMessage;
        this.reportedSystemAction = systemAction;
        this.reportedUserAction = userAction;
    }


    /**
     * This is the  constructor used for creating a ConnectionCheckedException that resulted from a previous error.
     *
     * @param httpCode - http response code to use if this exception flows over a rest call
     * @param className - name of class reporting error
     * @param actionDescription - description of function it was performing when error detected
     * @param errorMessage - description of error
     * @param systemAction - actions of the system as a result of the error
     * @param userAction - instructions for correcting the error
     * @param caughtError - the error that resulted in this exception.
     */
    public AssetConsumerCheckedExceptionBase(int       httpCode,
                                             String    className,
                                             String    actionDescription,
                                             String    errorMessage,
                                             String    systemAction,
                                             String    userAction,
                                             Throwable caughtError)
    {
        super(errorMessage, caughtError);
        this.reportedHTTPCode = httpCode;
        this.reportingClassName = className;
        this.reportingActionDescription = actionDescription;
        this.reportedErrorMessage = errorMessage;
        this.reportedSystemAction = systemAction;
        this.reportedUserAction = userAction;
        this.reportedCaughtException = caughtError;
    }


    /**
     * Return the HTTP response code to use with this exception.
     *
     * @return reportedHTTPCode
     */
    public int getReportedHTTPCode()
    {
        return reportedHTTPCode;
    }

    /**
     * The class that created this exception.
     *
     * @return reportingClassName
     */
    public String getReportingClassName()
    {
        return reportingClassName;
    }


    /**
     * The type of request that the class was performing when the condition occurred that resulted in this
     * exception.
     *
     * @return reportingActionDescription
     */
    public String getReportingActionDescription()
    {
        return reportingActionDescription;
    }


    /**
     * A formatted short description of the cause of the condition that resulted in this exception.
     *
     * @return reportedErrorMessage
     */
    public String getErrorMessage()
    {
        return reportedErrorMessage;
    }


    /**
     * A description of the action that the system took as a result of the error condition.
     *
     * @return reportedSystemAction
     */
    public String getReportedSystemAction()
    {
        return reportedSystemAction;
    }


    /**
     * A description of the action necessary to correct the error.
     *
     * @return reportedUserAction
     */
    public String getReportedUserAction()
    {
        return reportedUserAction;
    }


    /**
     * An exception that was caught and wrapped by this exception.  If a null is returned, then this exception is
     * newly created and not the result of a previous exception.
     *
     * @return reportedCaughtException
     */
    public Throwable getReportedCaughtException() { return reportedCaughtException; }
}
