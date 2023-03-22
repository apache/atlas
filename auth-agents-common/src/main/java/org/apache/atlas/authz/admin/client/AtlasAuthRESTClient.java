package org.apache.atlas.authz.admin.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.atlas.authorization.utils.StringUtil;
import org.apache.atlas.plugin.util.RangerRoles;
import org.apache.atlas.plugin.util.RangerUserStore;
import org.apache.atlas.plugin.util.ServicePolicies;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.client.utils.URIBuilder;

import javax.servlet.http.HttpServletResponse;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

public class AtlasAuthRESTClient implements AtlasAuthAdminClient {
    private static final Log LOG = LogFactory.getLog(AtlasAuthRESTClient.class);
    private static final int MAX_PLUGIN_ID_LEN = 255;
    private static final String SCHEME = "http";
    private String serviceName;
    private String pluginId;
    private OkHttpClient httpClient;
    private String adminUrl;


    @Override
    public void init(String serviceName, String appId, String configPropertyPrefix, Configuration config) {
        this.serviceName = serviceName;
        this.pluginId = getPluginId(serviceName, appId);
        String url = "";
        String tmpUrl = config.get(configPropertyPrefix + ".authz.rest.url");
        if (!StringUtil.isEmpty(tmpUrl)) {
            url = tmpUrl.trim();
        }
        if (url.endsWith("/")) {
            url = url.substring(0, url.length() - 1);
        }
        adminUrl = url;
        long tmpReadTimeout = config.getLong(configPropertyPrefix + ".policy.rest.client.read.timeoutMs", 20000);
        init(serviceName, tmpReadTimeout);
    }

    public String getPluginId(String serviceName, String appId) {
        String hostName = null;

        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            LOG.error("ERROR: Unable to find hostname for the agent ", e);
            hostName = "unknownHost";
        }

        String ret  = hostName + "-" + serviceName;

        if(! StringUtils.isEmpty(appId)) {
            ret = appId + "@" + ret;
        }

        if (ret.length() > MAX_PLUGIN_ID_LEN ) {
            ret = ret.substring(0,MAX_PLUGIN_ID_LEN);
        }

        return ret ;
    }

    private void init(String serviceName, long readTimeout) {
        this.serviceName = serviceName;
        this.httpClient = new OkHttpClient();
        this.httpClient.newBuilder().readTimeout(readTimeout, TimeUnit.MILLISECONDS);
    }


    @Override
    public ServicePolicies getServicePoliciesIfUpdated(long lastUpdatedTimeInMillis) throws Exception {
        URI uri = buildURI("/download/policies/" + serviceName, lastUpdatedTimeInMillis);
        return sendRequestAndGetResponse(uri, ServicePolicies.class);
    }

    @Override
    public RangerRoles getRolesIfUpdated(long lastUpdatedTimeInMillis) throws Exception {
        URI uri = buildURI("/download/roles/" + serviceName, lastUpdatedTimeInMillis);
        return sendRequestAndGetResponse(uri, RangerRoles.class);
    }

    @Override
    public RangerUserStore getUserStoreIfUpdated(long lastUpdatedTimeInMillis) throws Exception {
        URI uri = buildURI("/download/users/" + serviceName, lastUpdatedTimeInMillis);
        return sendRequestAndGetResponse(uri, RangerUserStore.class);
    }

    private <T> T sendRequestAndGetResponse(URI uri, Class<T> responseClass) throws Exception {
        Request request = new Request.Builder().url(uri.toURL()).build();
        Response response = httpClient.newCall(request).execute();

        if (response.code() == HttpServletResponse.SC_NO_CONTENT) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("<== AtlasAuthRESTClient.sendRequestAndGetResponse(): Not Modified");
            }
            return null;
        } else if (response.code() == HttpServletResponse.SC_OK) {
            String responseBody = response.body().string();
            if (StringUtils.isNotEmpty(responseBody)) {
                ObjectMapper mapper = new ObjectMapper();
                return mapper.readValue(responseBody, responseClass);
            } else {
                LOG.warn("AtlasAuthRESTClient.sendRequestAndGetResponse(): Empty response from Atlas Auth");
            }
        } else {
            LOG.error("AtlasAuthRESTClient.sendRequestAndGetResponse(): HTTP error: " + response.code());
        }
        return null;
    }

    private URI buildURI(String path, long lastUpdatedTimeInMillis) throws URISyntaxException {
        return new URIBuilder()
                .setScheme(SCHEME)
                .setHost(adminUrl)
                .setPath(path)
                .setParameter("lastUpdatedTime", String.valueOf(lastUpdatedTimeInMillis))
                .setParameter("pluginId", pluginId)
                .build();
    }
}
