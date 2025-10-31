package org.apache.atlas.web.integration.client;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.atlas.model.audit.EntityAuditSearchResult;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.web.integration.AtlasDockerIntegrationTest;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.atlas.web.integration.utils.TestConstants.DELETE_HANDLER_DEFAULT;
import static org.apache.atlas.web.integration.utils.TestUtil.fromJson;
import static org.apache.atlas.web.integration.utils.TestUtil.toJson;

public class OKClient {
    private static final Logger LOG = LoggerFactory.getLogger(OKClient.class);

    public static boolean isBeta = "beta".equals(ConfigReader.getString("atlas.client.mode", ""));

    private static String BASE_URL = AtlasDockerIntegrationTest.ATLAS_BASE_URL;

    private static String URL_TYPE_DEF = BASE_URL + "/types/typedefs";
    private static String URL_TYPE_DEF_BY_NAME = BASE_URL + "/types/typedef/name/%s";

    private static String URL_ENTITY = BASE_URL + "/entity";
    private static String URL_ENTITY_BULK = BASE_URL + "/entity/bulk?replaceTags=true";
    private static String URL_GET_ENTITY_GUID = BASE_URL + "/entity/guid/%s";

    private static String URL_UPSERT_BM = BASE_URL + "/entity/guid/%s/businessmetadata/%s";
    private static String URL_UPSERT_BM_BULK = BASE_URL + "/entity/guid/%s/businessmetadata";

    private static String URL_REPAIR_ASSET = BASE_URL + "/entity/guid/%s/repairindex";

    private static String URL_REPAIR_ASSETS = BASE_URL + "/entity/guid/bulk/repairindex";

    private static String URL_REPAIR_CLASSIFICATIONS_MAPPINGS = BASE_URL + "/entity/bulk/repairClassificationsMappings";

    private static String URL_INDEX_SEARCH = BASE_URL + "/search/indexsearch";

    private static String URL_RELATIONSHIP_BULK = BASE_URL + "/relationship/bulk";
    private static String URL_RELATIONSHIP = BASE_URL + "/relationship";
    private static String URL_RELATIONSHIP_DELETE_GUID = BASE_URL + "/relationship/guid/%s";


    private static String URL_ADD_TAGS_BY_TYPE_UNIQUE_ATTR = BASE_URL + "/entity/uniqueAttribute/type/%s/classifications?attr:qualifiedName=%s";
    private static String URL_DELETE_TAGS_BY_TYPE_UNIQUE_ATTR = BASE_URL + "/entity/uniqueAttribute/type/%s/classification/%s?attr:qualifiedName=%s";

    private static OkHttpClient finalClient = null;

    public static String ADMIN_TOKEN;

    private static long TIMEOUT  = 60000;
    private static TimeUnit TIME_UNIT = TimeUnit.SECONDS;

    public static OkHttpClient getClient() {
        if (finalClient == null) {
            synchronized (OKClient.class) {
                if (finalClient == null) {
                    finalClient = new OkHttpClient.Builder()
                            .connectTimeout(TIMEOUT, TIME_UNIT)
                            .readTimeout(TIMEOUT, TIME_UNIT)
                            .writeTimeout(TIMEOUT, TIME_UNIT)
                            .callTimeout(TIMEOUT, TIME_UNIT)
                            .build();
                }
            }
        }

        return finalClient;
    }

    public static String getToken() throws Exception {
        synchronized (OKClient.class) {
            if (StringUtils.isEmpty(ADMIN_TOKEN)) {
                if (StringUtils.isEmpty(ADMIN_TOKEN)) {
                    loadTokens();
                }
            }
        }

        return ADMIN_TOKEN;
    }

    private static void loadTokens() throws Exception {
        String auth = "admin:admin";
        ADMIN_TOKEN = "Basic " + Base64.getEncoder().encodeToString(auth.getBytes());
    }

    private static String getOrCreateToken(String conf, String userName) throws Exception {
        String token = ConfigReader.getString(conf);
        if (StringUtils.isEmpty(token)) {
            token = getKeycloakToken(userName);
        }

        return token;
    }

    private static Request getRequest(String url) throws Exception {
        return getRequest(url, null);
    }

    private static Request getRequestDELETE(String url) throws Exception {
        Request.Builder builder = new Request.Builder()
                .url(url)
                .delete()
                .header("Authorization", getToken())
                .header("Content-Type", "application/json")
                .header("x-atlan-request-id", "tests-2.0-client");

        return builder.build();
    }

    private static Request getRequest(String url, Object payload) throws Exception {

        Request.Builder builder = new Request.Builder()
                .url(url)
                .header("Authorization", getToken())
                .header("Content-Type", "application/json")
                .header("x-atlan-request-id", "tests-2.0-client");

        if (payload != null) {
            RequestBody body = RequestBody.create(toJson(payload), MediaType.get("application/json"));
            builder.post(body);
        }

        return builder.build();
    }

    private static Request getRequestPOST(String url, Object payload) throws Exception {

        Request.Builder builder = new Request.Builder()
                .url(url)
                .header("Authorization", getToken())
                .header("Content-Type", "application/json")
                .header("x-atlan-request-id", "tests-2.0-client");

        RequestBody body = okhttp3.RequestBody.create(new byte[0]);

        if (payload != null) {
            body = RequestBody.create(toJson(payload), MediaType.get("application/json"));
        }
        builder.post(body);

        return builder.build();
    }

    public AtlasSearchResult indexSearch(IndexSearchParams indexSearchParams) throws Exception {
        Request request = getRequest(URL_INDEX_SEARCH, indexSearchParams);

        return executeRequest(request, AtlasSearchResult.class);
    }

    public AtlasEntity.AtlasEntityWithExtInfo getEntityByGuid(String guid) throws Exception {
        Request request = getRequest(String.format(URL_GET_ENTITY_GUID, guid));

        return executeRequest(request, AtlasEntity.AtlasEntityWithExtInfo.class);
    }

    public EntityMutationResponse createEntity(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) throws Exception {
        Request request = getRequest(URL_ENTITY, entityWithExtInfo);

        return executeRequest(request, EntityMutationResponse.class);
    }

    public AtlasTypesDef createTypeDef(AtlasTypesDef typesDef) throws Exception {
        Request request = getRequest(URL_TYPE_DEF, typesDef);

        return executeRequest(request, AtlasTypesDef.class);
    }

    public AtlasTypesDef getBMDefs() throws Exception {
        Request request = getRequest(URL_TYPE_DEF + "?type=BUSINESS_METADATA");

        return executeRequest(request, AtlasTypesDef.class);
    }
    public AtlasTypesDef getTagDefs() throws Exception {
        Request request = getRequest(URL_TYPE_DEF + "?type=CLASSIFICATION");

        return executeRequest(request, AtlasTypesDef.class);
    }

    public void deleteTypeDefByName(String name) throws Exception {
        Request request = getRequestDELETE(String.format(URL_TYPE_DEF_BY_NAME, name));

        executeRequest(request, null);
    }

    public EntityMutationResponse createEntities(AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo) throws Exception {
        Request request = getRequest(URL_ENTITY_BULK, entitiesWithExtInfo);

        return executeRequest(request, EntityMutationResponse.class);
    }

    public EntityMutationResponse deleteEntityByGuid(String guid, String deleteType) throws Exception {
        String url = BASE_URL + "/entity/guid/" + guid;
        if (StringUtils.isNotEmpty(deleteType) && !DELETE_HANDLER_DEFAULT.equals(deleteType)) {
            url = url + "?deleteType=" + deleteType;
        }
        Request request = getRequestDELETE(url);

        return executeRequest(request, EntityMutationResponse.class);
    }

    public EntityMutationResponse deleteEntitiesByGuids(List<String> guids) throws Exception {
        if (CollectionUtils.isEmpty(guids)) {
            return null;
        }

        StringBuilder queryParams = new StringBuilder();
        queryParams.append(BASE_URL + "/entity/bulk?deleteType=PURGE&");
        guids.forEach(guid -> queryParams.append("guid=" + guid + "&"));

        String url = queryParams.toString();
        Request request = getRequestDELETE(url);

        return executeRequest(request, EntityMutationResponse.class);
    }

    public EntityAuditSearchResult getEntityAudit(Object params) throws Exception {
        if (params == null) {
            return null;
        }

        String url = BASE_URL + "/entity/auditSearch";
        Request request = getRequest(url, params);

        return executeRequest(request, EntityAuditSearchResult.class);
    }

    public void addOrUpdateCMAttrBulk(String assetGuid, Map<String, Object> bm) throws Exception {
        Request request = getRequest(String.format(URL_UPSERT_BM_BULK, assetGuid), bm);

        executeRequest(request, null);
    }

    public void repairEntityByGuid(String assetGuid) throws Exception {
        Request request = getRequestPOST(String.format(URL_REPAIR_ASSET, assetGuid), null);

        executeRequest(request, null);
    }

    public void repairEntitiesByGuid(List<String> assetGuids) throws Exception {
        Request request = getRequestPOST(URL_REPAIR_ASSETS, assetGuids);

        executeRequest(request, null);
    }

    public Map<String, String> repairClassificationsMappings(List<String> assetGuids) throws Exception {
        Request request = getRequestPOST(URL_REPAIR_CLASSIFICATIONS_MAPPINGS, assetGuids);

        return executeRequest(request, Map.class);
    }

    public AtlasRelationship createRelationship(AtlasRelationship relationship) throws Exception {
        Request request = getRequestPOST(URL_RELATIONSHIP, relationship);

        return executeRequest(request, AtlasRelationship.class);
    }

    public void deleteRelationshipByGuid(String relationshipGuid) throws Exception {
        Request request = getRequestDELETE(String.format(URL_RELATIONSHIP_DELETE_GUID, relationshipGuid));

        executeRequest(request, null);
    }

    public List<AtlasRelationship> createRelationships(List<AtlasRelationship> relationships) throws Exception {
        Request request = getRequestPOST(URL_RELATIONSHIP_BULK, relationships);

        return executeRequest(request, List.class);
    }

    public void addOrUpdateCMAttr(String assetGuid, String bmName, Map<String, Object> bm) throws Exception {
        Request request = getRequest(String.format(URL_UPSERT_BM, assetGuid, bmName), bm);

        executeRequest(request, null);
    }

    public Map<String, Object> searchTasks(Object searchRequest) throws Exception {
        String url = BASE_URL + "/task/search";
        Request request = getRequest(url, searchRequest);

        return executeRequest(request, Map.class);
    }

    public void addTagByTypeAPI(String entityTypeName, String entityQualifiedName,
                                List<AtlasClassification> classifications) throws Exception {
        String url = String.format(URL_ADD_TAGS_BY_TYPE_UNIQUE_ATTR, entityTypeName, entityQualifiedName);
        Request request = getRequest(url, classifications);

        executeRequest(request, null);
    }

    public void deleteTagByTypeAPI(String entityTypeName, String entityQualifiedName,
                                   String tagTypeName) throws Exception {
        String url = String.format(URL_DELETE_TAGS_BY_TYPE_UNIQUE_ATTR, entityTypeName, tagTypeName, entityQualifiedName);
        Request request = getRequestDELETE(url);

        executeRequest(request, null);
    }

    private <T> T executeRequest(Request request, Class<T> returnType) throws Exception {
        try (Response response = getClient().newCall(request).execute()) {
            if (response.code() == 200 || response.code() == 204) {
                if (returnType != null) {
                    return fromJson(response.body().string(), returnType);
                }
            } else {
                String responseBody = response.body() != null ? response.body().string() : "";
                String errorMessage = String.format("%d | %s | %s",
                        response.code(),
                        response.message(),
                        responseBody);
                LOG.error("Request failed: {}", errorMessage);
                throw new Exception(errorMessage);
            }
        }
        return null;
    }

    private static String getKeycloakToken(String userName) throws Exception {
        LOG.info("fetching token for {}", userName);
        String keycloakUrl = ConfigReader.getString("beta.client.host") + "/auth/realms/default/protocol/openid-connect/token";
        String clientSecret = ConfigReader.getString("beta.client.secret");

        Map<String, String> formData = new HashMap<>();
        formData.put("client_id", "atlan-argo");
        formData.put("client_secret", clientSecret);
        formData.put("grant_type", "client_credentials");

        if (StringUtils.isNotEmpty(userName)) {
            formData.put("username", userName);
            formData.put("password", ConfigReader.getString("beta.client.password"));
        }

        Request request = new Request.Builder()
                .url(keycloakUrl)
                .post(RequestBody.create(
                        MediaType.parse("application/x-www-form-urlencoded"),
                        formData.entrySet().stream()
                                .map(e -> e.getKey() + "=" + e.getValue())
                                .collect(Collectors.joining("&"))
                ))
                .build();

        try (Response response = OKClient.getClient().newCall(request).execute()) {
            if (response.code() == 200) {
                Map<String, Object> responseMap = fromJson(response.body().string(), Map.class);
                return responseMap.get("access_token") + "";
            } else {
                String errorBody = response.body() != null ? response.body().string() : "";
                throw new Exception("Failed to get token from Keycloak. Status: " + response.code() + ", Response: " + errorBody);
            }
        }
    }
}
