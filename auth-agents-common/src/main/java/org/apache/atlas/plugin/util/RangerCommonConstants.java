/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.atlas.plugin.util;

public class RangerCommonConstants {

	public static final String PROP_COOKIE_NAME                             = "ranger.admin.cookie.name";
	public static final String DEFAULT_COOKIE_NAME                          = "RANGERADMINSESSIONID";

	public static final String RANGER_ADMIN_SUFFIX_POLICY_DELTA             = ".supports.policy.deltas";
	public static final String PLUGIN_CONFIG_SUFFIX_POLICY_DELTA            = ".supports.policy.deltas";

	public static final String RANGER_ADMIN_SUFFIX_TAG_DELTA                = ".supports.tag.deltas";
	public static final String PLUGIN_CONFIG_SUFFIX_TAG_DELTA               = ".supports.tag.deltas";

	public static final String RANGER_ADMIN_SUFFIX_IN_PLACE_POLICY_UPDATES  = ".supports.in.place.policy.updates";
	public static final String PLUGIN_CONFIG_SUFFIX_IN_PLACE_POLICY_UPDATES = ".supports.in.place.policy.updates";

	public static final String RANGER_ADMIN_SUFFIX_IN_PLACE_TAG_UPDATES     = ".supports.in.place.tag.updates";
	public static final String PLUGIN_CONFIG_SUFFIX_IN_PLACE_TAG_UPDATES    = ".supports.in.place.tag.updates";

	public static final boolean RANGER_ADMIN_SUFFIX_POLICY_DELTA_DEFAULT             = false;
	public static final boolean PLUGIN_CONFIG_SUFFIX_POLICY_DELTA_DEFAULT            = false;

	public static final boolean RANGER_ADMIN_SUFFIX_TAG_DELTA_DEFAULT                = false;
	public static final boolean PLUGIN_CONFIG_SUFFIX_TAG_DELTA_DEFAULT               = false;

	public static final boolean RANGER_ADMIN_SUFFIX_IN_PLACE_POLICY_UPDATES_DEFAULT  = false;
	public static final boolean PLUGIN_CONFIG_SUFFIX_IN_PLACE_POLICY_UPDATES_DEFAULT = false;

	public static final boolean RANGER_ADMIN_SUFFIX_IN_PLACE_TAG_UPDATES_DEFAULT     = false;
	public static final boolean PLUGIN_CONFIG_SUFFIX_IN_PLACE_TAG_UPDATES_DEFAULT    = false;

	public static final boolean POLICY_REST_CLIENT_SESSION_COOKIE_ENABLED            = true;

	public static final String SCRIPT_OPTION_ENABLE_JSON_CTX        = "enableJsonCtx";
	public static final String SCRIPT_VAR_CONTEXT                   = "_ctx";
	public static final String SCRIPT_VAR_CONTEXT_JSON              = "_ctx_json";
	public static final String SCRIPT_FIELD_ACCESS_TIME             = "accessTime";
	public static final String SCRIPT_FIELD_ACCESS_TYPE             = "accessType";
	public static final String SCRIPT_FIELD_ACTION                  = "action";
	public static final String SCRIPT_FIELD_CLIENT_IP_ADDRESS       = "clientIPAddress";
	public static final String SCRIPT_FIELD_CLIENT_TYPE             = "clientType";
	public static final String SCRIPT_FIELD_CLUSTER_NAME            = "clusterName";
	public static final String SCRIPT_FIELD_CLUSTER_TYPE            = "clusterType";
	public static final String SCRIPT_FIELD_FORWARDED_ADDRESSES     = "forwardedAddresses";
	public static final String SCRIPT_FIELD_REMOTE_IP_ADDRESS       = "remoteIPAddress";
	public static final String SCRIPT_FIELD_REQUEST_DATA            = "requestData";
	public static final String SCRIPT_FIELD_RESOURCE                = "resource";
	public static final String SCRIPT_FIELD_RESOURCE_OWNER_USER     = "resourceOwnerUser";
	public static final String SCRIPT_FIELD_RESOURCE_MATCHING_SCOPE = "resourceMatchingScope";
	public static final String SCRIPT_FIELD_TAG                     = "tag";
	public static final String SCRIPT_FIELD_TAGS                    = "tags";
	public static final String SCRIPT_FIELD_USER                    = "user";
	public static final String SCRIPT_FIELD_USER_ATTRIBUTES         = "userAttributes";
	public static final String SCRIPT_FIELD_USER_GROUPS             = "userGroups";
	public static final String SCRIPT_FIELD_USER_GROUP_ATTRIBUTES   = "userGroupAttributes";
	public static final String SCRIPT_FIELD_USER_ROLES              = "userRoles";
	public static final String SCRIPT_FIELD_REQUEST                 = "request";
}
