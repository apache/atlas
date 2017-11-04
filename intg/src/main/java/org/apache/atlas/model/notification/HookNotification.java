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
package org.apache.atlas.model.notification;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;

import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.NONE;
import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * Base type of hook message.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.ALWAYS)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class HookNotification implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final String UNKNOW_USER = "UNKNOWN";

    /**
     * Type of the hook message.
     */
    public enum HookNotificationType {
        TYPE_CREATE, TYPE_UPDATE, ENTITY_CREATE, ENTITY_PARTIAL_UPDATE, ENTITY_FULL_UPDATE, ENTITY_DELETE
    }

    protected HookNotificationType type;
    protected String               user;

    public HookNotification() {
    }

    public HookNotification(HookNotificationType type, String user) {
        this.type = type;
        this.user = user;
    }

    public HookNotificationType getType() {
        return type;
    }

    public void setType(HookNotificationType type) {
        this.type = type;
    }

    public String getUser() {
        if (StringUtils.isEmpty(user)) {
            return UNKNOW_USER;
        }

        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void normalize() { }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }

    public StringBuilder toString(StringBuilder sb) {
        if (sb == null) {
            sb = new StringBuilder();
        }

        sb.append("HookNotification{");
        sb.append("type=").append(type);
        sb.append(", user=").append(user);
        sb.append("}");

        return sb;
    }
}
