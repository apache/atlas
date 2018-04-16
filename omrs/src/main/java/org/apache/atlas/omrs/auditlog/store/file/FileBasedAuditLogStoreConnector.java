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
package org.apache.atlas.omrs.auditlog.store.file;

import org.apache.atlas.ocf.ffdc.ConnectorCheckedException;
import org.apache.atlas.omrs.auditlog.store.OMRSAuditLogRecord;
import org.apache.atlas.omrs.auditlog.store.OMRSAuditLogStoreConnectorBase;
import org.apache.atlas.omrs.ffdc.OMRSErrorCode;
import org.apache.atlas.omrs.ffdc.exception.PagingErrorException;
import org.apache.atlas.omrs.ffdc.exception.InvalidParameterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * FileBasedAuditLogStoreConnector provides a connector implementation for a file based audit log.
 * The audit log is stored in a directory and each audit log record is stored as a file with a filename build
 * from the record's unique identifier (guid).
 */
public class FileBasedAuditLogStoreConnector extends OMRSAuditLogStoreConnectorBase
{
    private static final Logger log = LoggerFactory.getLogger(FileBasedAuditLogStoreConnector.class);


    /**
     * Default constructor used by the connector provider.
     */
    public FileBasedAuditLogStoreConnector()
    {
    }


    /**
     * Store the audit log record in the audit log store.
     *
     * @param logRecord - log record to store
     * @return unique identifier assigned to the log record
     * @throws InvalidParameterException - indicates that the logRecord parameter is invalid.
     */
    public String storeLogRecord(OMRSAuditLogRecord logRecord) throws InvalidParameterException
    {
        final String   methodName = "storeLogRecord";

        if (logRecord == null)
        {
            OMRSErrorCode errorCode    = OMRSErrorCode.NULL_LOG_RECORD;
            String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                                                this.getClass().getName(),
                                                methodName,
                                                errorMessage,
                                                errorCode.getSystemAction(),
                                                errorCode.getUserAction());
        }

        if (log.isDebugEnabled())
        {
            log.debug("AuditLogRecord: " + logRecord.toString());
        }

        return null;
    }


    /**
     * Retrieve a specific audit log record.
     *
     * @param logRecordId - unique identifier for the log record
     * @return requested audit log record
     * @throws InvalidParameterException - indicates that the logRecordId parameter is invalid.
     */
    public OMRSAuditLogRecord  getAuditLogRecord(String     logRecordId) throws InvalidParameterException
    {

        return null;
    }


    /**
     * Retrieve a list of log records written in a specified time period.  The offset and maximumRecords
     * parameters support a paging
     *
     * @param startDate - start of time period
     * @param endDate - end of time period
     * @param offset - offset of full collection to begin the return results
     * @param maximumRecords - maximum number of log records to return
     * @return list of log records from the specified time period
     * @throws InvalidParameterException - indicates that the start and/or end date parameters are invalid.
     * @throws PagingErrorException - indicates that the offset or the maximumRecords parameters are invalid.
     */
    public ArrayList<OMRSAuditLogRecord> getAuditLogRecordsByTimeStamp(Date    startDate,
                                                                       Date    endDate,
                                                                       int     offset,
                                                                       int     maximumRecords) throws InvalidParameterException,
                                                                                                      PagingErrorException
    {
        return null;
    }

    /**
     * Retrieve a list of log records of a specific severity.  The offset and maximumRecords
     * parameters support a paging model.
     *
     * @param severity - the severity value of messages to return
     * @param startDate - start of time period
     * @param endDate - end of time period
     * @param offset - offset of full collection to begin the return results
     * @param maximumRecords - maximum number of log records to return
     * @return list of log records from the specified time period
     * @throws InvalidParameterException - indicates that the severity, start and/or end date parameters are invalid.
     * @throws PagingErrorException - indicates that the offset or the maximumRecords parameters are invalid.
     */
    public ArrayList<OMRSAuditLogRecord> getAuditLogRecordsBySeverity(String   severity,
                                                                      Date     startDate,
                                                                      Date     endDate,
                                                                      int      offset,
                                                                      int      maximumRecords) throws InvalidParameterException,
                                                                                                      PagingErrorException
    {
        return null;
    }


    /**
     * Retrieve a list of log records written by a specific component.  The offset and maximumRecords
     * parameters support a paging model.
     *
     * @param component - name of the component to retrieve events from
     * @param startDate - start of time period
     * @param endDate - end of time period
     * @param offset - offset of full collection to begin the return results
     * @param maximumRecords - maximum number of log records to return
     * @return list of log records from the specified time period
     * @throws InvalidParameterException - indicates that the component, start and/or end date parameters are invalid.
     * @throws PagingErrorException - indicates that the offset or the maximumRecords parameters are invalid.
     */
    public List<OMRSAuditLogRecord> getAuditLogRecordsByComponent(String component,
                                                                  Date   startDate,
                                                                  Date   endDate,
                                                                  int    offset,
                                                                  int    maximumRecords) throws InvalidParameterException,
                                                                                                PagingErrorException
    {
        return null;
    }


    /**
     * Indicates that the connector is completely configured and can begin processing.
     *
     * @throws ConnectorCheckedException - there is a problem within the connector.
     */
    public void start() throws ConnectorCheckedException
    {
        super.start();
    }


    /**
     * Free up any resources held since the connector is no longer needed.
     *
     * @throws ConnectorCheckedException - there is a problem within the connector.
     */
    public  void disconnect() throws ConnectorCheckedException
    {
        super.disconnect();
    }
}
