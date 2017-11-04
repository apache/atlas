/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.hbase.model;

import org.apache.atlas.hbase.bridge.HBaseAtlasHook;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.ArrayList;
import java.util.List;

import java.util.Map;

public class HBaseOperationContext {
    private final UserGroupInformation     ugi;
    private final Map<String, String>      hbaseConf;
    private final HBaseAtlasHook.OPERATION operation;
    private final String                   user;
    private final NamespaceDescriptor      namespaceDescriptor;
    private final HTableDescriptor         hTableDescriptor;
    private final HColumnDescriptor[]      hColumnDescriptors;
    private final TableName                tableName;
    private final String                   nameSpace;
    private final String                   columnFamily;
    private final String                   owner;
    private final HColumnDescriptor        hColumnDescriptor;

    public HBaseOperationContext(NamespaceDescriptor namespaceDescriptor, String nameSpace, HTableDescriptor hTableDescriptor, TableName tableName, HColumnDescriptor[] hColumnDescriptors,
                          HColumnDescriptor hColumnDescriptor, String columnFamily, HBaseAtlasHook.OPERATION operation, UserGroupInformation ugi , String user, String owner,
                          Map<String, String> hbaseConf) {
        this.namespaceDescriptor    = namespaceDescriptor;
        this.nameSpace              = nameSpace;
        this.hTableDescriptor       = hTableDescriptor;
        this.tableName              = tableName;
        this.hColumnDescriptors     = hColumnDescriptors;
        this.hColumnDescriptor      = hColumnDescriptor;
        this.columnFamily           = columnFamily;
        this.operation              = operation;
        this.ugi                    = ugi;
        this.user                   = user;
        this.owner                  = owner;
        this.hbaseConf              = hbaseConf;
    }

    public  HBaseOperationContext(NamespaceDescriptor namespaceDescriptor, String nameSpace, HBaseAtlasHook.OPERATION operation, UserGroupInformation ugi , String user, String owner) {
        this(namespaceDescriptor, nameSpace, null, null, null, null, null, operation, ugi, user, owner, null);
    }

    public  HBaseOperationContext(String nameSpace, HTableDescriptor hTableDescriptor, TableName tableName,  HColumnDescriptor[] hColumnDescriptor, HBaseAtlasHook.OPERATION operation, UserGroupInformation ugi, String user, String owner, Map<String,String> hbaseConf) {
        this(null, nameSpace, hTableDescriptor, tableName, hColumnDescriptor, null, null, operation, ugi, user, owner, hbaseConf);
    }

    public  HBaseOperationContext(String nameSpace, TableName tableName, HColumnDescriptor hColumnDescriptor, String columnFamily, HBaseAtlasHook.OPERATION operation, UserGroupInformation ugi, String user, String owner, Map<String,String> hbaseConf) {
        this(null, nameSpace, null, tableName, null, hColumnDescriptor, columnFamily, operation, ugi, user, owner, hbaseConf);
    }

    private List<HookNotification> messages = new ArrayList<>();

    public UserGroupInformation getUgi() {
        return ugi;
    }

    public Map<String, String>  getHbaseConf() {
        return hbaseConf;
    }

    public String getUser() {
        return user;
    }

    public HBaseAtlasHook.OPERATION getOperation() {
        return operation;
    }

    public NamespaceDescriptor getNamespaceDescriptor() {
        return namespaceDescriptor;
    }

    public HTableDescriptor gethTableDescriptor() {
        return hTableDescriptor;
    }

    public HColumnDescriptor[] gethColumnDescriptors() {
        return hColumnDescriptors;
    }

    public TableName getTableName() {
        return tableName;
    }

    public String getNameSpace() {
        return nameSpace;
    }

    public HColumnDescriptor gethColumnDescriptor() {
        return hColumnDescriptor;
    }

    public String getColummFamily() {
        return columnFamily;
    }

    public void addMessage(HookNotification message) {
        messages.add(message);
    }

    public String getOwner() {
        return owner;
    }

    public List<HookNotification> getMessages() {
        return messages;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        toString(sb);
        return sb.toString();
    }

    public StringBuilder toString(StringBuilder sb) {
        sb.append("HBaseOperationContext={");
        sb.append("Operation={").append(operation).append("} ");
        sb.append("User ={").append(user).append("} ");
        if (nameSpace != null ) {
            sb.append("NameSpace={").append(nameSpace).append("}");
        } else {
            if (namespaceDescriptor != null) {
                sb.append("NameSpace={").append(namespaceDescriptor.toString()).append("}");
            }
        }
        if (tableName != null ) {
            sb.append("Table={").append(tableName).append("}");
        } else {
            if ( hColumnDescriptor != null) {
                sb.append("Table={").append(hTableDescriptor.toString()).append("}");
            }
        }
        if (columnFamily != null ) {
            sb.append("Columm Family={").append(columnFamily).append("}");
        } else {
            if ( hColumnDescriptor != null) {
                sb.append("Columm Family={").append(hColumnDescriptor.toString()).append("}");
            }
        }
        sb.append("Message ={").append(getMessages()).append("} ");
        sb.append(" }");
        return sb;
    }

}
