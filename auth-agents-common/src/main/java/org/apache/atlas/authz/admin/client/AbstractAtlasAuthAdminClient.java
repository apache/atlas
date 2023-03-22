package org.apache.atlas.authz.admin.client;

import org.apache.atlas.plugin.util.RangerRoles;
import org.apache.atlas.plugin.util.RangerUserStore;
import org.apache.atlas.plugin.util.ServicePolicies;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractAtlasAuthAdminClient implements AtlasAuthAdminClient {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractAtlasAuthAdminClient.class);

    @Override
    public void init(String serviceName, String appId, String configPropertyPrefix, Configuration config) {
        // TODO Any initialization required?
    }

    @Override
    public ServicePolicies getServicePoliciesIfUpdated(long lastUpdatedTimeInMillis) throws Exception {
        LOG.error("getServicePoliciesIfUpdated() not implemented");
        return null;
    }

    @Override
    public RangerRoles getRolesIfUpdated(long lastUpdatedTimeInMillis) throws Exception {
        LOG.error("getRolesIfUpdated() not implemented");
        return null;
    }

    @Override
    public RangerUserStore getUserStoreIfUpdated(long lastUpdateTimeInMillis) throws Exception {
        LOG.error("getUserStoreIfUpdated() not implemented");
        return null;
    }

}
