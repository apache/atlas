/**
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

package org.apache.atlas.utils;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Optional;

public class AtlasPathExtractorUtil {
    // Common
    public static final char   QNAME_SEP_METADATA_NAMESPACE = '@';
    public static final char   QNAME_SEP_ENTITY_NAME        = '.';
    public static final String SCHEME_SEPARATOR             = "://";
    public static final String ATTRIBUTE_QUALIFIED_NAME     = "qualifiedName";
    public static final String ATTRIBUTE_NAME               = "name";
    public static final String ATTRIBUTE_BUCKET             = "bucket";
    // HDFS
    public static final String HDFS_TYPE_PATH           = "hdfs_path";
    public static final String ATTRIBUTE_PATH           = "path";
    public static final String ATTRIBUTE_CLUSTER_NAME   = "clusterName";
    public static final String ATTRIBUTE_NAMESERVICE_ID = "nameServiceId";
    // AWS S3
    public static final String AWS_S3_ATLAS_MODEL_VERSION_V2              = "v2";
    public static final String AWS_S3_BUCKET                              = "aws_s3_bucket";
    public static final String AWS_S3_PSEUDO_DIR                          = "aws_s3_pseudo_dir";
    public static final String AWS_S3_V2_BUCKET                           = "aws_s3_v2_bucket";
    public static final String AWS_S3_V2_PSEUDO_DIR                       = "aws_s3_v2_directory";
    public static final String S3_SCHEME                                  = "s3" + SCHEME_SEPARATOR;
    public static final String S3A_SCHEME                                 = "s3a" + SCHEME_SEPARATOR;
    public static final String ATTRIBUTE_CONTAINER                        = "container";
    public static final String ATTRIBUTE_OBJECT_PREFIX                    = "objectPrefix";
    public static final String RELATIONSHIP_AWS_S3_BUCKET_S3_PSEUDO_DIRS  = "aws_s3_bucket_aws_s3_pseudo_dirs";
    public static final String RELATIONSHIP_AWS_S3_V2_CONTAINER_CONTAINED = "aws_s3_v2_container_contained";
    // ADLS Gen2
    public static final String ADLS_GEN2_ACCOUNT                         = "adls_gen2_account";
    public static final String ADLS_GEN2_CONTAINER                       = "adls_gen2_container";
    public static final String ADLS_GEN2_DIRECTORY                       = "adls_gen2_directory";
    public static final String ADLS_GEN2_ACCOUNT_HOST_SUFFIX             = ".dfs.core.windows.net";
    public static final String ABFS_SCHEME                               = "abfs" + SCHEME_SEPARATOR;
    public static final String ABFSS_SCHEME                              = "abfss" + SCHEME_SEPARATOR;
    public static final String ATTRIBUTE_ACCOUNT                         = "account";
    public static final String ATTRIBUTE_PARENT                          = "parent";
    public static final String RELATIONSHIP_ADLS_GEN2_ACCOUNT_CONTAINERS = "adls_gen2_account_containers";
    public static final String RELATIONSHIP_ADLS_GEN2_PARENT_CHILDREN    = "adls_gen2_parent_children";
    // Ozone
    public static final String OZONE_VOLUME                       = "ozone_volume";
    public static final String OZONE_BUCKET                       = "ozone_bucket";
    public static final String OZONE_KEY                          = "ozone_key";
    public static final String OZONE_SCHEME                       = "ofs" + SCHEME_SEPARATOR;
    public static final String OZONE_3_SCHEME                     = "o3fs" + SCHEME_SEPARATOR;
    public static final String ATTRIBUTE_VOLUME                   = "volume";
    public static final String RELATIONSHIP_OZONE_VOLUME_BUCKET   = "ozone_volume_buckets";
    public static final String RELATIONSHIP_OZONE_PARENT_CHILDREN = "ozone_parent_children";
    public static final String OZONE_SCHEME_NAME                  = "ofs";
    //Google Cloud Storage
    public static final String GCS_SCHEME                       = "gs" + SCHEME_SEPARATOR;
    public static final String GCS_BUCKET                       = "gcp_storage_bucket";
    public static final String GCS_VIRTUAL_DIR                  = "gcp_storage_virtual_directory";
    public static final String ATTRIBUTE_GCS_PARENT             = "parent";
    public static final String RELATIONSHIP_GCS_PARENT_CHILDREN = "gcp_storage_parent_children";
    private static final Logger LOG = LoggerFactory.getLogger(AtlasPathExtractorUtil.class);

    private AtlasPathExtractorUtil() {
        // to block instantiation
    }

    public static AtlasEntityWithExtInfo getPathEntity(Path path, PathExtractorContext context) {
        AtlasEntityWithExtInfo entityWithExtInfo = new AtlasEntityWithExtInfo();
        String                 strPath           = path.toString();
        AtlasEntity            ret;

        if (context.isConvertPathToLowerCase()) {
            strPath = strPath.toLowerCase();
        }

        if (isS3Path(strPath)) {
            ret = isAwsS3AtlasModelVersionV2(context) ? addS3PathEntityV2(path, entityWithExtInfo, context) : addS3PathEntityV1(path, entityWithExtInfo, context);
        } else if (isAbfsPath(strPath)) {
            ret = addAbfsPathEntity(path, entityWithExtInfo, context);
        } else if (isOzonePath(strPath)) {
            ret = addOzonePathEntity(path, entityWithExtInfo, context);
        } else if (isGCSPath(strPath)) {
            ret = addGCSPathEntity(path, entityWithExtInfo, context);
        } else {
            ret = addHDFSPathEntity(path, context);
        }

        entityWithExtInfo.setEntity(ret);

        return entityWithExtInfo;
    }

    private static boolean isAwsS3AtlasModelVersionV2(PathExtractorContext context) {
        return StringUtils.isNotEmpty(context.getAwsS3AtlasModelVersion()) && StringUtils.equalsIgnoreCase(context.getAwsS3AtlasModelVersion(), AWS_S3_ATLAS_MODEL_VERSION_V2);
    }

    private static boolean isS3Path(String strPath) {
        return strPath != null && (strPath.startsWith(S3_SCHEME) || strPath.startsWith(S3A_SCHEME));
    }

    private static boolean isAbfsPath(String strPath) {
        return strPath != null && (strPath.startsWith(ABFS_SCHEME) || strPath.startsWith(ABFSS_SCHEME));
    }

    private static boolean isOzonePath(String strPath) {
        return strPath != null && (strPath.startsWith(OZONE_SCHEME) || strPath.startsWith(OZONE_3_SCHEME));
    }

    private static boolean isGCSPath(String strPath) {
        return strPath != null && strPath.startsWith(GCS_SCHEME);
    }

    private static AtlasEntity addS3PathEntityV1(Path path, AtlasEntityExtInfo extInfo, PathExtractorContext context) {
        String strPath = path.toString();

        LOG.debug("==> addS3PathEntityV1(strPath={})", strPath);

        String      metadataNamespace   = context.getMetadataNamespace();
        String      bucketName          = path.toUri().getAuthority();
        String      bucketQualifiedName = (path.toUri().getScheme() + SCHEME_SEPARATOR + path.toUri().getAuthority() + QNAME_SEP_METADATA_NAMESPACE).toLowerCase() + metadataNamespace;
        String      pathQualifiedName   = (strPath + QNAME_SEP_METADATA_NAMESPACE).toLowerCase() + metadataNamespace;
        AtlasEntity bucketEntity        = context.getEntity(bucketQualifiedName);
        AtlasEntity ret                 = context.getEntity(pathQualifiedName);

        if (ret == null) {
            if (bucketEntity == null) {
                bucketEntity = new AtlasEntity(AWS_S3_BUCKET);

                bucketEntity.setAttribute(ATTRIBUTE_QUALIFIED_NAME, bucketQualifiedName);
                bucketEntity.setAttribute(ATTRIBUTE_NAME, bucketName);

                context.putEntity(bucketQualifiedName, bucketEntity);
            }

            extInfo.addReferredEntity(bucketEntity);

            ret = new AtlasEntity(AWS_S3_PSEUDO_DIR);

            ret.setRelationshipAttribute(ATTRIBUTE_BUCKET, AtlasTypeUtil.getAtlasRelatedObjectId(bucketEntity, RELATIONSHIP_AWS_S3_BUCKET_S3_PSEUDO_DIRS));
            ret.setAttribute(ATTRIBUTE_OBJECT_PREFIX, Path.getPathWithoutSchemeAndAuthority(path).toString().toLowerCase());
            ret.setAttribute(ATTRIBUTE_QUALIFIED_NAME, pathQualifiedName);
            ret.setAttribute(ATTRIBUTE_NAME, Path.getPathWithoutSchemeAndAuthority(path).toString().toLowerCase());

            context.putEntity(pathQualifiedName, ret);
        }

        LOG.debug("<== addS3PathEntityV1(strPath={})", strPath);

        return ret;
    }

    private static AtlasEntity addS3PathEntityV2(Path path, AtlasEntityExtInfo extInfo, PathExtractorContext context) {
        String strPath = path.toString();

        LOG.debug("==> addS3PathEntityV2(strPath={})", strPath);

        String      metadataNamespace = context.getMetadataNamespace();
        String      pathQualifiedName = strPath + QNAME_SEP_METADATA_NAMESPACE + metadataNamespace;
        AtlasEntity ret               = context.getEntity(pathQualifiedName);

        if (ret == null) {
            String      bucketName          = path.toUri().getAuthority();
            String      schemeAndBucketName = (path.toUri().getScheme() + SCHEME_SEPARATOR + bucketName).toLowerCase();
            String      bucketQualifiedName = schemeAndBucketName + QNAME_SEP_METADATA_NAMESPACE + metadataNamespace;
            AtlasEntity bucketEntity        = context.getEntity(bucketQualifiedName);

            if (bucketEntity == null) {
                bucketEntity = new AtlasEntity(AWS_S3_V2_BUCKET);

                bucketEntity.setAttribute(ATTRIBUTE_QUALIFIED_NAME, bucketQualifiedName);
                bucketEntity.setAttribute(ATTRIBUTE_NAME, bucketName);

                LOG.debug("adding entity: typeName={}, qualifiedName={}", bucketEntity.getTypeName(), bucketEntity.getAttribute(ATTRIBUTE_QUALIFIED_NAME));

                context.putEntity(bucketQualifiedName, bucketEntity);
            }

            extInfo.addReferredEntity(bucketEntity);

            AtlasRelatedObjectId parentObjId = AtlasTypeUtil.getAtlasRelatedObjectId(bucketEntity, RELATIONSHIP_AWS_S3_V2_CONTAINER_CONTAINED);
            String               parentPath  = Path.SEPARATOR;
            String               dirPath     = path.toUri().getPath();

            if (StringUtils.isEmpty(dirPath)) {
                dirPath = Path.SEPARATOR;
            }

            for (String subDirName : dirPath.split(Path.SEPARATOR)) {
                if (StringUtils.isEmpty(subDirName)) {
                    continue;
                }

                String subDirPath          = parentPath + subDirName + Path.SEPARATOR;
                String subDirQualifiedName = schemeAndBucketName + subDirPath + QNAME_SEP_METADATA_NAMESPACE + metadataNamespace;

                ret = context.getEntity(subDirQualifiedName);

                if (ret == null) {
                    ret = new AtlasEntity(AWS_S3_V2_PSEUDO_DIR);

                    ret.setRelationshipAttribute(ATTRIBUTE_CONTAINER, parentObjId);
                    ret.setAttribute(ATTRIBUTE_OBJECT_PREFIX, subDirPath);
                    ret.setAttribute(ATTRIBUTE_QUALIFIED_NAME, subDirQualifiedName);
                    ret.setAttribute(ATTRIBUTE_NAME, subDirName);

                    LOG.debug("adding entity: typeName={}, qualifiedName={}", ret.getTypeName(), ret.getAttribute(ATTRIBUTE_QUALIFIED_NAME));

                    context.putEntity(subDirQualifiedName, ret);
                }

                parentObjId = AtlasTypeUtil.getAtlasRelatedObjectId(ret, RELATIONSHIP_AWS_S3_V2_CONTAINER_CONTAINED);
                parentPath  = subDirPath;
            }

            if (ret == null) {
                ret = bucketEntity;
            }
        }

        LOG.debug("<== addS3PathEntityV2(strPath={})", strPath);

        return ret;
    }

    private static AtlasEntity addAbfsPathEntity(Path path, AtlasEntityExtInfo extInfo, PathExtractorContext context) {
        String strPath = path.toString();

        LOG.debug("==> addAbfsPathEntity(strPath={})", strPath);

        String      metadataNamespace = context.getMetadataNamespace();
        String      pathQualifiedName = strPath + QNAME_SEP_METADATA_NAMESPACE + metadataNamespace;
        AtlasEntity ret               = context.getEntity(pathQualifiedName);

        if (ret == null) {
            String      abfsScheme               = path.toUri().getScheme();
            String      storageAcctName          = getAbfsStorageAccountName(path.toUri());
            String      schemeAndStorageAcctName = (abfsScheme + SCHEME_SEPARATOR + storageAcctName).toLowerCase();
            String      storageAcctQualifiedName = schemeAndStorageAcctName + QNAME_SEP_METADATA_NAMESPACE + metadataNamespace;
            AtlasEntity storageAcctEntity        = context.getEntity(storageAcctQualifiedName);

            // create adls-gen2 storage-account entity
            if (storageAcctEntity == null) {
                storageAcctEntity = new AtlasEntity(ADLS_GEN2_ACCOUNT);

                storageAcctEntity.setAttribute(ATTRIBUTE_QUALIFIED_NAME, storageAcctQualifiedName);
                storageAcctEntity.setAttribute(ATTRIBUTE_NAME, storageAcctName);

                LOG.debug("adding entity: typeName={}, qualifiedName={}", storageAcctEntity.getTypeName(), storageAcctEntity.getAttribute(ATTRIBUTE_QUALIFIED_NAME));

                context.putEntity(storageAcctQualifiedName, storageAcctEntity);
            }

            extInfo.addReferredEntity(storageAcctEntity);

            AtlasRelatedObjectId storageAcctObjId = AtlasTypeUtil.getAtlasRelatedObjectId(storageAcctEntity, RELATIONSHIP_ADLS_GEN2_ACCOUNT_CONTAINERS);

            // create adls-gen2 container entity linking to storage account
            String      containerName          = path.toUri().getUserInfo();
            String      schemeAndContainerName = (abfsScheme + SCHEME_SEPARATOR + containerName + QNAME_SEP_METADATA_NAMESPACE + storageAcctName).toLowerCase();
            String      containerQualifiedName = schemeAndContainerName + QNAME_SEP_METADATA_NAMESPACE + metadataNamespace;
            AtlasEntity containerEntity        = context.getEntity(containerQualifiedName);

            if (containerEntity == null) {
                containerEntity = new AtlasEntity(ADLS_GEN2_CONTAINER);

                containerEntity.setAttribute(ATTRIBUTE_QUALIFIED_NAME, containerQualifiedName);
                containerEntity.setAttribute(ATTRIBUTE_NAME, containerName);
                containerEntity.setRelationshipAttribute(ATTRIBUTE_ACCOUNT, storageAcctObjId);

                LOG.debug("adding entity: typeName={}, qualifiedName={}", containerEntity.getTypeName(), containerEntity.getAttribute(ATTRIBUTE_QUALIFIED_NAME));

                context.putEntity(containerQualifiedName, containerEntity);
            }

            extInfo.addReferredEntity(containerEntity);

            // create adls-gen2 directory entity linking to container
            AtlasRelatedObjectId parentObjId = AtlasTypeUtil.getAtlasRelatedObjectId(containerEntity, RELATIONSHIP_ADLS_GEN2_PARENT_CHILDREN);
            String               parentPath  = Path.SEPARATOR;
            String               dirPath     = path.toUri().getPath();

            if (StringUtils.isEmpty(dirPath)) {
                dirPath = Path.SEPARATOR;
            }

            for (String subDirName : dirPath.split(Path.SEPARATOR)) {
                if (StringUtils.isEmpty(subDirName)) {
                    continue;
                }

                String subDirPath          = parentPath + subDirName;
                String subDirQualifiedName = schemeAndContainerName + subDirPath + QNAME_SEP_METADATA_NAMESPACE + metadataNamespace;

                ret = context.getEntity(subDirQualifiedName);

                if (ret == null) {
                    ret = new AtlasEntity(ADLS_GEN2_DIRECTORY);

                    ret.setRelationshipAttribute(ATTRIBUTE_PARENT, parentObjId);
                    ret.setAttribute(ATTRIBUTE_QUALIFIED_NAME, subDirQualifiedName);
                    ret.setAttribute(ATTRIBUTE_NAME, subDirName);

                    LOG.debug("adding entity: typeName={}, qualifiedName={}", ret.getTypeName(), ret.getAttribute(ATTRIBUTE_QUALIFIED_NAME));

                    context.putEntity(subDirQualifiedName, ret);
                }

                parentObjId = AtlasTypeUtil.getAtlasRelatedObjectId(ret, RELATIONSHIP_ADLS_GEN2_PARENT_CHILDREN);
                parentPath  = subDirPath + Path.SEPARATOR;
            }

            if (ret == null) {
                ret = storageAcctEntity;
            }
        }

        LOG.debug("<== addAbfsPathEntity(strPath={})", strPath);

        return ret;
    }

    private static String o3fsAuthorityExtractor(String path) {
        return Optional.ofNullable(path)
                .filter(p -> p.startsWith("o3fs://"))
                .map(p -> p.substring(7).split("/", 2))
                .filter(parts -> parts.length > 0)
                .map(parts -> parts[0])
                .orElse("");
    }

    private static String[] o3fsKeyExtractor(String path) {
        return Optional.ofNullable(path)
                .filter(p -> p.startsWith("o3fs://"))
                .map(p -> p.substring(7).split("/", 2))
                .filter(parts -> parts.length == 2)
                .map(parts -> parts[1].split("/"))
                .orElse(new String[] {""});
    }

    private static int getO3fsPathLength(String path) {
        if (path == null || !path.startsWith("o3fs://")) {
            return 0;
        }
        String noScheme = path.substring("o3fs://".length());
        String[] parts = noScheme.split("/", 2);
        String authority = parts[0];
        String keyPart = parts.length > 1 ? parts[1].trim() : "";

        // Count bucket and volume from authority
        String[] authorityParts = authority.split("\\.");
        int length = 0;
        if (authorityParts.length >= 1) {
            length++;  // bucket
        }
        if (authorityParts.length >= 2) {
            length++;  // volume
        }

        // Count key segments if present
        if (!keyPart.isEmpty()) {
            String[] keySegments = keyPart.split("/");
            length += keySegments.length;
        }

        return length;
    }

    private static AtlasEntity createOzoneEntity(PathExtractorContext context, String typeName, String name, String qualifiedName, AtlasRelatedObjectId relationship) {
        AtlasEntity ozoneEntity = context.getEntity(qualifiedName);

        if (ozoneEntity == null) {
            ozoneEntity = new AtlasEntity(typeName);
            ozoneEntity.setAttribute(ATTRIBUTE_QUALIFIED_NAME, qualifiedName);
            ozoneEntity.setAttribute(ATTRIBUTE_NAME, name);

            if (relationship != null) {
                String relationshipAttribute = typeName.equals(OZONE_BUCKET) ? ATTRIBUTE_VOLUME : ATTRIBUTE_PARENT;
                ozoneEntity.setRelationshipAttribute(relationshipAttribute, relationship);
            }

            context.putEntity(qualifiedName, ozoneEntity);
            LOG.info("Added entity: typeName={}, qualifiedName={}", typeName, qualifiedName);
        }

        return ozoneEntity;
    }

    private static AtlasEntity addOfsPathEntity(Path path, AtlasEntityExtInfo extInfo, PathExtractorContext context) {
        String  metadataNamespace   = context.getMetadataNamespace();
        String  ozoneScheme         = path.toUri().getScheme();
        String  volumeName          = getOzoneVolumeName(path);
        String  volumeQualifiedName =  OZONE_SCHEME + volumeName + QNAME_SEP_METADATA_NAMESPACE + metadataNamespace;

        String dirPath = path.toUri().getPath();
        if (StringUtils.isEmpty(dirPath)) {
            dirPath = Path.SEPARATOR;
        }

        String[] subDirNames = dirPath.split(Path.SEPARATOR);
        if (subDirNames.length < 2) {
            LOG.error("Path Missing: {}", AtlasErrorCode.INCORRECT_OZONE_PATH.getFormattedErrorMessage());
            return null;
        }

        AtlasEntity volumeEntity = createOzoneEntity(context, OZONE_VOLUME, volumeName, volumeQualifiedName, null);
        extInfo.addReferredEntity(volumeEntity);

        if (subDirNames.length == 2) {
            return volumeEntity;
        }

        String bucket = subDirNames[2];
        String bucketQN = OZONE_SCHEME + volumeName + QNAME_SEP_ENTITY_NAME + bucket + QNAME_SEP_METADATA_NAMESPACE + metadataNamespace;
        AtlasEntity bucketEntity = createOzoneEntity(context, OZONE_BUCKET, bucket, bucketQN, AtlasTypeUtil.getAtlasRelatedObjectId(volumeEntity, RELATIONSHIP_OZONE_VOLUME_BUCKET));
        extInfo.addReferredEntity(bucketEntity);

        if (subDirNames.length == 3) {
            return bucketEntity;
        }

        AtlasEntity   currentKey     = null;
        StringBuilder keyPathBuilder = new StringBuilder();
        String        keyQNamePrefix = ozoneScheme + SCHEME_SEPARATOR + path.toUri().getAuthority();

        AtlasEntity parent = bucketEntity;

        for (int i = 3; i < subDirNames.length; i++) {
            String dir = subDirNames[i];
            if (StringUtils.isEmpty(dir)) {
                continue;
            }

            keyPathBuilder.append(Path.SEPARATOR).append(dir);

            String subDirQualifiedName = keyQNamePrefix
                    + Path.SEPARATOR + volumeName
                    + Path.SEPARATOR + bucket
                    + keyPathBuilder
                    + QNAME_SEP_METADATA_NAMESPACE + metadataNamespace;

            currentKey = createOzoneEntity(context, OZONE_KEY, dir, subDirQualifiedName,
                    AtlasTypeUtil.getAtlasRelatedObjectId(parent, RELATIONSHIP_OZONE_PARENT_CHILDREN));
            parent = currentKey;
            AtlasTypeUtil.getAtlasRelatedObjectId(parent, RELATIONSHIP_OZONE_PARENT_CHILDREN);
        }
        return currentKey;
    }

    private static AtlasEntity addO3fsPathEntity(Path path, AtlasEntityExtInfo extInfo, PathExtractorContext context) {
        String[]    o3fsKeyName         = o3fsKeyExtractor(path.toString());
        int         pathLength          = getO3fsPathLength(path.toString());
        String      metadataNamespace   = context.getMetadataNamespace();
        String      ozoneScheme          = path.toUri().getScheme();
        String      bucketName          = getOzoneBucketName(path);
        String      volumeName          = getOzoneVolumeName(path);
        String      volumeQualifiedName = ozoneScheme + SCHEME_SEPARATOR + volumeName + QNAME_SEP_METADATA_NAMESPACE + metadataNamespace;

        AtlasEntity volumeEntity = createOzoneEntity(context, OZONE_VOLUME, volumeName, volumeQualifiedName, null);
        extInfo.addReferredEntity(volumeEntity);

        String bucketQualifiedName = ozoneScheme + SCHEME_SEPARATOR + volumeName + Path.CUR_DIR + bucketName + QNAME_SEP_METADATA_NAMESPACE + metadataNamespace;

        AtlasEntity bucketEntity = createOzoneEntity(context, OZONE_BUCKET, bucketName, bucketQualifiedName, AtlasTypeUtil.getAtlasRelatedObjectId(volumeEntity, RELATIONSHIP_OZONE_VOLUME_BUCKET));
        extInfo.addReferredEntity(bucketEntity);

        if (pathLength < 1) {
            LOG.error("Path Missing: {}", AtlasErrorCode.INCORRECT_OZONE_PATH.getFormattedErrorMessage());
            return null;
        }

        if (pathLength == 1) {
            return volumeEntity;
        }
        if (pathLength == 2) {
            return bucketEntity;
        }

        AtlasEntity currentKey = null;
        StringBuilder keyPathBuilder = new StringBuilder();
        String authority = o3fsAuthorityExtractor(path.toString());

        for (String key : o3fsKeyName) {
            keyPathBuilder.append(Path.SEPARATOR).append(key);
            String keyQN = ozoneScheme + SCHEME_SEPARATOR + authority + keyPathBuilder + QNAME_SEP_METADATA_NAMESPACE + metadataNamespace;
            currentKey = createOzoneEntity(context, OZONE_KEY, key, keyQN,
                    AtlasTypeUtil.getAtlasRelatedObjectId(bucketEntity, RELATIONSHIP_OZONE_PARENT_CHILDREN));
            bucketEntity = currentKey;
            AtlasTypeUtil.getAtlasRelatedObjectId(bucketEntity, RELATIONSHIP_OZONE_PARENT_CHILDREN);
        }

        return currentKey;
    }

    private static AtlasEntity addOzonePathEntity(Path path, AtlasEntityExtInfo extInfo, PathExtractorContext context) {
        if (path == null) {
            LOG.error("Invalid input: path is null");
            return null;
        }

        String strPath = path.toString();

        if (StringUtils.isEmpty(strPath)) {
            LOG.error("Invalid input: strPath is empty");
            return null;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> addOzonePathEntity(strPath={})", strPath);
        }

        return strPath.startsWith(OZONE_3_SCHEME)
                ? addO3fsPathEntity(path, extInfo, context)
                : addOfsPathEntity(path, extInfo, context);
    }

    private static AtlasEntity addGCSPathEntity(Path path, AtlasEntityExtInfo extInfo, PathExtractorContext context) {
        String strPath = path.toString();

        LOG.debug("==> addGCSPathEntity(strPath={})", strPath);

        String      metadataNamespace = context.getMetadataNamespace();
        String      pathQualifiedName = strPath + QNAME_SEP_METADATA_NAMESPACE + metadataNamespace;
        AtlasEntity ret               = context.getEntity(pathQualifiedName);

        if (ret == null) {
            String      bucketName          = path.toUri().getAuthority();
            String      schemeAndBucketName = (path.toUri().getScheme() + SCHEME_SEPARATOR + bucketName).toLowerCase();
            String      bucketQualifiedName = schemeAndBucketName + QNAME_SEP_METADATA_NAMESPACE + metadataNamespace;
            AtlasEntity bucketEntity        = context.getEntity(bucketQualifiedName);

            if (bucketEntity == null) {
                bucketEntity = new AtlasEntity(GCS_BUCKET);

                bucketEntity.setAttribute(ATTRIBUTE_QUALIFIED_NAME, bucketQualifiedName);
                bucketEntity.setAttribute(ATTRIBUTE_NAME, bucketName);

                LOG.debug("adding entity: typeName={}, qualifiedName={}", bucketEntity.getTypeName(), bucketEntity.getAttribute(ATTRIBUTE_QUALIFIED_NAME));

                context.putEntity(bucketQualifiedName, bucketEntity);
            }

            extInfo.addReferredEntity(bucketEntity);

            AtlasRelatedObjectId parentObjId = AtlasTypeUtil.getAtlasRelatedObjectId(bucketEntity, RELATIONSHIP_GCS_PARENT_CHILDREN);
            String               parentPath  = Path.SEPARATOR;
            String               dirPath     = path.toUri().getPath();

            if (StringUtils.isEmpty(dirPath)) {
                dirPath = Path.SEPARATOR;
            }

            for (String subDirName : dirPath.split(Path.SEPARATOR)) {
                if (StringUtils.isEmpty(subDirName)) {
                    continue;
                }

                String subDirPath          = parentPath + subDirName + Path.SEPARATOR;
                String subDirQualifiedName = schemeAndBucketName + subDirPath + QNAME_SEP_METADATA_NAMESPACE + metadataNamespace;

                ret = context.getEntity(subDirQualifiedName);

                if (ret == null) {
                    ret = new AtlasEntity(GCS_VIRTUAL_DIR);

                    ret.setRelationshipAttribute(ATTRIBUTE_GCS_PARENT, parentObjId);
                    ret.setAttribute(ATTRIBUTE_OBJECT_PREFIX, parentPath);
                    ret.setAttribute(ATTRIBUTE_QUALIFIED_NAME, subDirQualifiedName);
                    ret.setAttribute(ATTRIBUTE_NAME, subDirName);

                    LOG.debug("adding entity: typeName={}, qualifiedName={}", ret.getTypeName(), ret.getAttribute(ATTRIBUTE_QUALIFIED_NAME));

                    context.putEntity(subDirQualifiedName, ret);
                }

                parentObjId = AtlasTypeUtil.getAtlasRelatedObjectId(ret, RELATIONSHIP_GCS_PARENT_CHILDREN);
                parentPath  = subDirPath;
            }

            if (ret == null) {
                ret = bucketEntity;
            }
        }

        LOG.debug("<== addGCSPathEntity(strPath={})", strPath);

        return ret;
    }

    private static AtlasEntity addHDFSPathEntity(Path path, PathExtractorContext context) {
        String strPath = path.toString();

        if (context.isConvertPathToLowerCase()) {
            strPath = strPath.toLowerCase();
        }

        LOG.debug("==> addHDFSPathEntity(strPath={})", strPath);

        String      nameServiceID     = HdfsNameServiceResolver.getNameServiceIDForPath(strPath);
        String      attrPath          = StringUtils.isEmpty(nameServiceID) ? strPath : HdfsNameServiceResolver.getPathWithNameServiceID(strPath);
        String      pathQualifiedName = getQualifiedName(attrPath, context.getMetadataNamespace());
        AtlasEntity ret               = context.getEntity(pathQualifiedName);

        if (ret == null) {
            ret = new AtlasEntity(HDFS_TYPE_PATH);

            if (StringUtils.isNotEmpty(nameServiceID)) {
                ret.setAttribute(ATTRIBUTE_NAMESERVICE_ID, nameServiceID);
            }

            String name = Path.getPathWithoutSchemeAndAuthority(path).toString();

            if (context.isConvertPathToLowerCase()) {
                name = name.toLowerCase();
            }

            ret.setAttribute(ATTRIBUTE_PATH, attrPath);
            ret.setAttribute(ATTRIBUTE_QUALIFIED_NAME, pathQualifiedName);
            ret.setAttribute(ATTRIBUTE_NAME, name);
            ret.setAttribute(ATTRIBUTE_CLUSTER_NAME, context.getMetadataNamespace());

            context.putEntity(pathQualifiedName, ret);
        }

        LOG.debug("<== addHDFSPathEntity(strPath={})", strPath);

        return ret;
    }

    private static String getAbfsStorageAccountName(URI uri) {
        String ret  = null;
        String host = uri.getHost();

        // host: "<account_name>.dfs.core.windows.net"
        if (StringUtils.isNotEmpty(host) && host.contains(ADLS_GEN2_ACCOUNT_HOST_SUFFIX)) {
            ret = host.substring(0, host.indexOf(ADLS_GEN2_ACCOUNT_HOST_SUFFIX));
        }

        return ret;
    }

    private static String getOzoneVolumeName(Path path) {
        String strPath    = path.toString();
        String volumeName = StringUtils.EMPTY;

        if (strPath.startsWith(OZONE_3_SCHEME)) {
            String pathAuthority = path.toUri().getAuthority();
            volumeName = pathAuthority.split("\\.")[1];
        } else if (strPath.startsWith(OZONE_SCHEME)) {
            strPath = strPath.replaceAll(OZONE_SCHEME, StringUtils.EMPTY);

            if (strPath.split(Path.SEPARATOR).length >= 2) {
                volumeName = strPath.split(Path.SEPARATOR)[1];
            }
        }

        return volumeName;
    }

    private static String getOzoneBucketName(Path path) {
        String strPath    = path.toString();
        String bucketName = StringUtils.EMPTY;

        if (strPath.startsWith(OZONE_3_SCHEME)) {
            String pathAuthority = path.toUri().getAuthority();

            bucketName = pathAuthority.split("\\.")[0];
        } else if (strPath.startsWith(OZONE_SCHEME)) {
            strPath = strPath.replaceAll(OZONE_SCHEME, StringUtils.EMPTY);

            if (strPath.split(Path.SEPARATOR).length >= 3) {
                bucketName = strPath.split(Path.SEPARATOR)[2];
            }
        }

        return bucketName;
    }

    private static String getQualifiedName(String path, String metadataNamespace) {
        if (path.startsWith(HdfsNameServiceResolver.HDFS_SCHEME)) {
            return path + QNAME_SEP_METADATA_NAMESPACE + metadataNamespace;
        }

        return path.toLowerCase();
    }
}
