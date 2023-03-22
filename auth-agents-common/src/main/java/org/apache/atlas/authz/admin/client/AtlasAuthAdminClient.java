package org.apache.atlas.authz.admin.client;

import org.apache.atlas.plugin.util.RangerRoles;
import org.apache.atlas.plugin.util.RangerUserStore;
import org.apache.atlas.plugin.util.ServicePolicies;
import org.apache.hadoop.conf.Configuration;

public interface AtlasAuthAdminClient {
    void init(String serviceName, String appId, String configPropertyPrefix, Configuration config);

    ServicePolicies getServicePoliciesIfUpdated(long lastUpdatedTimeInMillis) throws Exception;

    RangerRoles getRolesIfUpdated(long lastUpdatedTimeInMillis) throws Exception;

    RangerUserStore getUserStoreIfUpdated(long lastUpdateTimeInMillis) throws Exception;
}
