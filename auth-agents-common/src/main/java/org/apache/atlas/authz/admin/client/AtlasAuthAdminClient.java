package org.apache.atlas.authz.admin.client;

import org.apache.atlas.authorization.hadoop.config.RangerPluginConfig;
import org.apache.atlas.plugin.util.RangerRoles;
import org.apache.atlas.plugin.util.RangerUserStore;
import org.apache.atlas.plugin.util.ServicePolicies;

public interface AtlasAuthAdminClient {
    void init(RangerPluginConfig config);

    ServicePolicies getServicePoliciesIfUpdated(long lastUpdatedTimeInMillis, boolean usePolicyDelta) throws Exception;

    RangerRoles getRolesIfUpdated(long lastUpdatedTimeInMillis) throws Exception;

    RangerUserStore getUserStoreIfUpdated(long lastUpdateTimeInMillis) throws Exception;
}
