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
package org.apache.atlas.omrs.auditlog.store;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * OMRSAuditLogReportingComponent describes the component issuing the audit log record.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class OMRSAuditLogReportingComponent
{
    private  int      componentId = 0;
    private  String   componentName = null;
    private  String   componentDescription = null;
    private  String   componentWikiURL = null;


    /**
     * Construct the description of the reporting component.
     *
     * @param componentId - numerical identifier for the component.
     * @param componentName - display name for the component.
     * @param componentDescription - description of the component.
     * @param componentWikiURL - link to more information.
     */
    public OMRSAuditLogReportingComponent(int componentId,
                                          String componentName,
                                          String componentDescription,
                                          String componentWikiURL)
    {
        this.componentId = componentId;
        this.componentName = componentName;
        this.componentDescription = componentDescription;
        this.componentWikiURL = componentWikiURL;
    }

    /**
     * Return the numerical code for this enum.
     *
     * @return int componentId
     */
    public int getComponentId()
    {
        return componentId;
    }


    /**
     * Return the name of the component.  This is the name used in the audit log records.
     *
     * @return String component name
     */
    public String getComponentName()
    {
        return componentName;
    }


    /**
     * Return the short description of the component. This is an English description.  Natural language support for
     * these values can be added to UIs using a resource bundle indexed with the component Id.  This value is
     * provided as a default if the resource bundle is not available.
     *
     * @return String description
     */
    public String getComponentDescription()
    {
        return componentDescription;
    }


    /**
     * URL link to the wiki page that describes this component.  This provides more information to the log reader
     * on the operation of the component.
     *
     * @return String URL
     */
    public String getComponentWikiURL()
    {
        return componentWikiURL;
    }


    @Override
    public String toString()
    {
        return  "ReportingComponent { " +
                "id : " + this.componentId + ", " +
                "name : " + this.componentName + ", " +
                "description : " + this.componentDescription + ", " +
                "url : " + this.componentWikiURL + " }";
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        OMRSAuditLogReportingComponent that = (OMRSAuditLogReportingComponent) o;

        if (componentId != that.componentId)
        {
            return false;
        }
        if (componentName != null ? !componentName.equals(that.componentName) : that.componentName != null)
        {
            return false;
        }
        if (componentDescription != null ? !componentDescription.equals(that.componentDescription) : that.componentDescription != null)
        {
            return false;
        }
        return componentWikiURL != null ? componentWikiURL.equals(that.componentWikiURL) : that.componentWikiURL == null;
    }

    @Override
    public int hashCode()
    {
        int result = componentId;
        result = 31 * result + (componentName != null ? componentName.hashCode() : 0);
        result = 31 * result + (componentDescription != null ? componentDescription.hashCode() : 0);
        result = 31 * result + (componentWikiURL != null ? componentWikiURL.hashCode() : 0);
        return result;
    }
}
