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

package org.apache.atlas.hbase.util;

import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.utils.AuthenticationUtil;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ImportHBaseEntitiesBase {
    private static final Logger LOG = LoggerFactory.getLogger(ImportHBaseEntitiesBase.class);

    static final String NAMESPACE_FLAG         = "-n";
    static final String TABLE_FLAG             = "-t";
    static final String NAMESPACE_FULL_FLAG    = "--namespace";
    static final String TABLE_FULL_FLAG        = "--tablename";
    static final String  ATLAS_ENDPOINT        = "atlas.rest.address";
    static final String  DEFAULT_ATLAS_URL     = "http://localhost:21000/";
    static final String  NAMESPACE_TYPE        = "hbase_namespace";
    static final String  TABLE_TYPE            = "hbase_table";
    static final String  COLUMNFAMILY_TYPE     = "hbase_column_family";
    static final String  HBASE_CLUSTER_NAME    = "atlas.cluster.name";
    static final String  DEFAULT_CLUSTER_NAME  = "primary";
    static final String  QUALIFIED_NAME        = "qualifiedName";
    static final String  NAME                  = "name";
    static final String  URI                   = "uri";
    static final String  OWNER                 = "owner";
    static final String  DESCRIPTION_ATTR      = "description";
    static final String  CLUSTERNAME           = "clusterName";
    static final String  NAMESPACE             = "namespace";
    static final String  TABLE                 = "table";
    static final String  COLUMN_FAMILIES       = "column_families";


    protected final HBaseAdmin                   hbaseAdmin;
    protected final boolean                      failOnError;
    protected final String                       namespaceToImport;
    protected final String                       tableToImport;
    private   final AtlasClientV2                atlasClientV2;
    private   final UserGroupInformation         ugi;
    private   final String                       clusterName;
    private   final HashMap<String, AtlasEntity> nameSpaceCache     = new HashMap<>();
    private   final HashMap<String, AtlasEntity> tableCache         = new HashMap<>();
    private   final HashMap<String, AtlasEntity> columnFamilyCache  = new HashMap<>();


    protected ImportHBaseEntitiesBase(String[] args) throws Exception {
        checkArgs(args);

        Configuration atlasConf = ApplicationProperties.get();
        String[]      urls      = atlasConf.getStringArray(ATLAS_ENDPOINT);

        if (urls == null || urls.length == 0) {
            urls = new String[]{DEFAULT_ATLAS_URL};
        }

        if (!AuthenticationUtil.isKerberosAuthenticationEnabled()) {
            String[] basicAuthUsernamePassword = AuthenticationUtil.getBasicAuthenticationInput();

            ugi           = null;
            atlasClientV2 = new AtlasClientV2(urls, basicAuthUsernamePassword);
        } else {
            ugi           = UserGroupInformation.getCurrentUser();
            atlasClientV2 = new AtlasClientV2(ugi, ugi.getShortUserName(), urls);
        }

        Options options = new Options();
        options.addOption("n","namespace", true, "namespace");
        options.addOption("t", "table", true, "tablename");
        options.addOption("failOnError", false, "failOnError");

        CommandLineParser parser  = new BasicParser();
        CommandLine       cmd     = parser.parse(options, args);

        clusterName       = atlasConf.getString(HBASE_CLUSTER_NAME, DEFAULT_CLUSTER_NAME);
        failOnError       = cmd.hasOption("failOnError");
        namespaceToImport = cmd.getOptionValue("n");
        tableToImport     = cmd.getOptionValue("t");

        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();

        LOG.info("createHBaseClient(): checking HBase availability..");

        HBaseAdmin.checkHBaseAvailable(conf);

        LOG.info("createHBaseClient(): HBase is available");

        hbaseAdmin = new HBaseAdmin(conf);
    }

    protected AtlasEntity createOrUpdateNameSpace(NamespaceDescriptor namespaceDescriptor) throws Exception {
        String      nsName          = namespaceDescriptor.getName();
        String      nsQualifiedName = getNameSpaceQualifiedName(clusterName, nsName);
        AtlasEntity nsEntity        = findNameSpaceEntityInAtlas(nsQualifiedName);

        if (nsEntity == null) {
            LOG.info("Importing NameSpace: " + nsQualifiedName);

            AtlasEntity entity = getNameSpaceEntity(nsName);

            nsEntity = createEntityInAtlas(entity);
        }

        return nsEntity;
    }

    protected  AtlasEntity createOrUpdateTable(String nameSpace, String tableName, AtlasEntity nameSapceEntity, HTableDescriptor htd, HColumnDescriptor[] hcdts) throws Exception {
        String      owner            = htd.getOwnerString();
        String      tblQualifiedName = getTableQualifiedName(clusterName, nameSpace, tableName);
        AtlasEntity tableEntity      = findTableEntityInAtlas(tblQualifiedName);

        if (tableEntity == null) {
            LOG.info("Importing Table: " + tblQualifiedName);

            AtlasEntity entity = getTableEntity(nameSpace, tableName, owner, nameSapceEntity);

            tableEntity = createEntityInAtlas(entity);
        }

        List<AtlasEntity>   cfEntities = createOrUpdateColumnFamilies(nameSpace, tableName, owner, hcdts, tableEntity);
        List<AtlasObjectId> cfIDs      = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(cfEntities)) {
            for (AtlasEntity cfEntity : cfEntities) {
                cfIDs.add(AtlasTypeUtil.getAtlasObjectId(cfEntity));
            }
        }

        tableEntity.setAttribute(COLUMN_FAMILIES, cfIDs);

        return tableEntity;
    }

    protected List<AtlasEntity> createOrUpdateColumnFamilies(String nameSpace, String tableName, String owner, HColumnDescriptor[] hcdts , AtlasEntity tableEntity) throws Exception {
        List<AtlasEntity> ret = new ArrayList<>();

        if (hcdts != null) {
            AtlasObjectId tableId = AtlasTypeUtil.getAtlasObjectId(tableEntity);

            for (HColumnDescriptor hcdt : hcdts) {
                String      cfName          = hcdt.getNameAsString();
                String      cfQualifiedName = getColumnFamilyQualifiedName(clusterName, nameSpace, tableName, cfName);
                AtlasEntity cfEntity        = findColumnFamiltyEntityInAtlas(cfQualifiedName);

                if (cfEntity == null) {
                    LOG.info("Importing Column-family: " + cfQualifiedName);

                    AtlasEntity entity = getColumnFamilyEntity(nameSpace, tableName, owner, hcdt, tableId);

                    cfEntity = createEntityInAtlas(entity);
                }

                ret.add(cfEntity);
            }
        }

        return ret;
    }

    private AtlasEntity findNameSpaceEntityInAtlas(String nsQualifiedName) {
        AtlasEntity ret = nameSpaceCache.get(nsQualifiedName);

        if (ret == null) {
            try {
                ret = findEntityInAtlas(NAMESPACE_TYPE, nsQualifiedName);

                if (ret != null) {
                    nameSpaceCache.put(nsQualifiedName, ret);
                }
            } catch (Exception e) {
                ret = null; // entity doesn't exist in Atlas
            }
        }

        return ret;
    }

    private AtlasEntity findTableEntityInAtlas(String tableQualifiedName) {
        AtlasEntity ret = tableCache.get(tableQualifiedName);

        if (ret == null) {
            try {
                ret = findEntityInAtlas(TABLE_TYPE, tableQualifiedName);

                if (ret != null) {
                    tableCache.put(tableQualifiedName, ret);
                }
            } catch (Exception e) {
                ret = null; // entity doesn't exist in Atlas
            }
        }

        return ret;
    }

    private AtlasEntity findColumnFamiltyEntityInAtlas(String columnFamilyQualifiedName) {
        AtlasEntity ret = columnFamilyCache.get(columnFamilyQualifiedName);

        if (ret == null) {
            try {
                ret = findEntityInAtlas(COLUMNFAMILY_TYPE, columnFamilyQualifiedName);

                if (ret != null) {
                    columnFamilyCache.put(columnFamilyQualifiedName, ret);
                }
            } catch (Exception e) {
                ret = null; // entity doesn't exist in Atlas
            }
        }

        return ret;
    }

    private AtlasEntity findEntityInAtlas(String typeName, String qualifiedName) throws Exception {
        Map<String, String> attributes = Collections.singletonMap(QUALIFIED_NAME, qualifiedName);

        return atlasClientV2.getEntityByAttribute(typeName, attributes).getEntity();
    }

    private AtlasEntity getNameSpaceEntity(String nameSpace){
        AtlasEntity ret           = new AtlasEntity(NAMESPACE_TYPE);
        String      qualifiedName = getNameSpaceQualifiedName(clusterName, nameSpace);

        ret.setAttribute(QUALIFIED_NAME, qualifiedName);
        ret.setAttribute(CLUSTERNAME, clusterName);
        ret.setAttribute(NAME, nameSpace);
        ret.setAttribute(DESCRIPTION_ATTR, nameSpace);

        return ret;
    }

    private AtlasEntity getTableEntity(String nameSpace, String tableName, String owner, AtlasEntity nameSpaceEntity) {
        AtlasEntity ret                = new AtlasEntity(TABLE_TYPE);
        String      tableQualifiedName = getTableQualifiedName(clusterName, nameSpace, tableName);

        ret.setAttribute(QUALIFIED_NAME, tableQualifiedName);
        ret.setAttribute(CLUSTERNAME, clusterName);
        ret.setAttribute(NAMESPACE, AtlasTypeUtil.getAtlasObjectId(nameSpaceEntity));
        ret.setAttribute(NAME, tableName);
        ret.setAttribute(DESCRIPTION_ATTR, tableName);
        ret.setAttribute(OWNER, owner);
        ret.setAttribute(URI, tableName);

        return ret;
    }

    private AtlasEntity getColumnFamilyEntity(String nameSpace, String tableName, String owner, HColumnDescriptor hcdt, AtlasObjectId tableId){
        AtlasEntity ret             = new AtlasEntity(COLUMNFAMILY_TYPE);
        String      cfName          = hcdt.getNameAsString();
        String      cfQualifiedName = getColumnFamilyQualifiedName(clusterName, nameSpace, tableName, cfName);

        ret.setAttribute(QUALIFIED_NAME, cfQualifiedName);
        ret.setAttribute(CLUSTERNAME, clusterName);
        ret.setAttribute(TABLE, tableId);
        ret.setAttribute(NAME, cfName);
        ret.setAttribute(DESCRIPTION_ATTR, cfName);
        ret.setAttribute(OWNER, owner);

        return ret;
    }

    private AtlasEntity createEntityInAtlas(AtlasEntity entity) throws Exception {
        AtlasEntity             ret      = null;
        EntityMutationResponse  response = atlasClientV2.createEntity(new AtlasEntity.AtlasEntityWithExtInfo(entity));
        List<AtlasEntityHeader> entities = response.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE);

        if (CollectionUtils.isNotEmpty(entities)) {
            AtlasEntity.AtlasEntityWithExtInfo getByGuidResponse = atlasClientV2.getEntityByGuid(entities.get(0).getGuid());

            ret = getByGuidResponse.getEntity();

            LOG.info ("Created entity: type=" + ret.getTypeName() + ", guid=" + ret.getGuid());
        }

        return ret;
    }

    private void checkArgs(String[] args) throws Exception {
        String option = args.length > 0 ? args[0] : null;
        String value  = args.length > 1 ? args[1] : null;

        if (option != null && value == null) {
            if (option.equalsIgnoreCase(NAMESPACE_FLAG) || option.equalsIgnoreCase(NAMESPACE_FULL_FLAG) ||
                option.equalsIgnoreCase(TABLE_FLAG) || option.equalsIgnoreCase(TABLE_FULL_FLAG)) {

                System.out.println("Usage: import-hbase.sh [-n <namespace> OR --namespace <namespace>] [-t <table> OR --table <table>]");

                throw new Exception("Incorrect arguments..");
            }
        }
    }

    /**
     * Construct the qualified name used to uniquely identify a ColumnFamily instance in Atlas.
     * @param clusterName Name of the cluster to which the Hbase component belongs
     * @param nameSpace Name of the Hbase database to which the Table belongs
     * @param tableName Name of the Hbase table
     * @param columnFamily Name of the ColumnFamily
     * @return Unique qualified name to identify the Table instance in Atlas.
     */
    private static String getColumnFamilyQualifiedName(String clusterName, String nameSpace, String tableName, String columnFamily) {
        tableName = stripNameSpace(tableName.toLowerCase());

        return String.format("%s.%s.%s@%s", nameSpace.toLowerCase(), tableName, columnFamily.toLowerCase(), clusterName);
    }

    /**
     * Construct the qualified name used to uniquely identify a Table instance in Atlas.
     * @param clusterName Name of the cluster to which the Hbase component belongs
     * @param nameSpace Name of the Hbase database to which the Table belongs
     * @param tableName Name of the Hbase table
     * @return Unique qualified name to identify the Table instance in Atlas.
     */
    private static String getTableQualifiedName(String clusterName, String nameSpace, String tableName) {
        tableName = stripNameSpace(tableName.toLowerCase());

        return String.format("%s.%s@%s", nameSpace.toLowerCase(), tableName, clusterName);
    }

    /**
     * Construct the qualified name used to uniquely identify a Hbase NameSpace instance in Atlas.
     * @param clusterName Name of the cluster to which the Hbase component belongs
     * @param nameSpace Name of the NameSpace
     * @return Unique qualified name to identify the HBase NameSpace instance in Atlas.
     */
    private static String getNameSpaceQualifiedName(String clusterName, String nameSpace) {
        return String.format("%s@%s", nameSpace.toLowerCase(), clusterName);
    }

    private static String stripNameSpace(String tableName){
        tableName = tableName.substring(tableName.indexOf(":")+1);

        return tableName;
    }
}
