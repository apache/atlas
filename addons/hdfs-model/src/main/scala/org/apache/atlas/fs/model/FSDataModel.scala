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
package org.apache.atlas.fs.model

import org.apache.atlas.{AtlasConstants, AtlasClient}
import org.apache.atlas.typesystem.TypesDef
import org.apache.atlas.typesystem.builders.TypesBuilder
import org.apache.atlas.typesystem.json.TypesSerialization
import org.apache.atlas.typesystem.types.DataTypes.MapType
import org.apache.hadoop.fs.permission.FsAction

import scala.tools.scalap.scalax.rules.scalasig.ClassFileParser.EnumConstValue

/**
 * This represents the data model for a HDFS Path
 */
object FSDataModel extends App {

    val typesBuilder = new TypesBuilder
    import typesBuilder._

    val typesDef : TypesDef = types {

        // FS DataSet
        _class(FSDataTypes.FS_PATH.toString, List(AtlasClient.DATA_SET_SUPER_TYPE)) {
            //fully qualified path/URI to the filesystem path is stored in 'qualifiedName' and 'path'.
            "path" ~ (string, required, indexed)
            "createTime" ~ (date, optional, indexed)
            "modifiedTime" ~ (date, optional, indexed)
            //Is a regular file or a directory. If true, it is a file else a directory
            "isFile" ~ (boolean, optional, indexed)
            //Is a symlink or not
            "isSymlink" ~ (boolean, optional, indexed)
            //Optional and may not be set for a directory
            "fileSize" ~ (long, optional, indexed)
            "group" ~ (string, optional, indexed)
            "posixPermissions" ~ (FSDataTypes.FS_PERMISSIONS.toString, optional, indexed)
        }

        enum(FSDataTypes.FS_ACTION.toString,  FsAction.values().map(x => x.name()) : _*)

        struct(FSDataTypes.FS_PERMISSIONS.toString) {
            PosixPermissions.PERM_USER.toString ~ (FSDataTypes.FS_ACTION.toString, required, indexed)
            PosixPermissions.PERM_GROUP.toString ~ (FSDataTypes.FS_ACTION.toString, required, indexed)
            PosixPermissions.PERM_OTHER.toString ~ (FSDataTypes.FS_ACTION.toString, required, indexed)
            PosixPermissions.STICKY_BIT.toString ~ (boolean, required, indexed)
        }

        //HDFS DataSet
        _class(FSDataTypes.HDFS_PATH.toString, List(FSDataTypes.FS_PATH.toString)) {
            //Making cluster optional since path is already unique containing the namenode URI
            AtlasConstants.CLUSTER_NAME_ATTRIBUTE ~ (string, optional, indexed)
            "numberOfReplicas" ~ (int, optional, indexed)
            "extendedAttributes" ~ (map(string, string), optional, indexed)
        }
        //TODO - ACLs - https://hadoop.apache.org/docs/r2.7.1/hadoop-project-dist/hadoop-hdfs/HdfsPermissionsGuide.html#ACLs_Access_Control_Lists
    }

    // add the types to atlas
    val typesAsJSON = TypesSerialization.toJson(typesDef)
    println("FS Data Model as JSON: ")
    println(typesAsJSON)

}

object FSDataTypes extends Enumeration {
    type FSDataTypes = Value
    val FS_ACTION = Value("file_action")
    val FS_PATH = Value("fs_path")
    val HDFS_PATH = Value("hdfs_path")
    val FS_PERMISSIONS = Value("fs_permissions")
}

object PosixPermissions extends Enumeration {
    type PosixPermissions = Value
    val PERM_USER = Value("user")
    val PERM_GROUP = Value("group")
    val PERM_OTHER = Value("others")
    val STICKY_BIT = Value("sticky")
}
