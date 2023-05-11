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

package org.apache.atlas.plugin.conditionevaluator;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.atlas.authorization.utils.JsonUtils;
import org.apache.atlas.authorization.utils.StringUtil;
import org.apache.atlas.plugin.contextenricher.RangerTagForEval;
import org.apache.atlas.plugin.policyengine.RangerAccessRequest;
import org.apache.atlas.plugin.policyengine.RangerAccessResource;
import org.apache.atlas.plugin.util.RangerAccessRequestUtil;
import org.apache.atlas.plugin.util.RangerPerfTracer;
import org.apache.atlas.plugin.util.RangerUserStore;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import static org.apache.atlas.plugin.util.RangerCommonConstants.SCRIPT_FIELD_ACCESS_TIME;
import static org.apache.atlas.plugin.util.RangerCommonConstants.SCRIPT_FIELD_ACCESS_TYPE;
import static org.apache.atlas.plugin.util.RangerCommonConstants.SCRIPT_FIELD_ACTION;
import static org.apache.atlas.plugin.util.RangerCommonConstants.SCRIPT_FIELD_CLIENT_IP_ADDRESS;
import static org.apache.atlas.plugin.util.RangerCommonConstants.SCRIPT_FIELD_CLIENT_TYPE;
import static org.apache.atlas.plugin.util.RangerCommonConstants.SCRIPT_FIELD_CLUSTER_NAME;
import static org.apache.atlas.plugin.util.RangerCommonConstants.SCRIPT_FIELD_CLUSTER_TYPE;
import static org.apache.atlas.plugin.util.RangerCommonConstants.SCRIPT_FIELD_FORWARDED_ADDRESSES;
import static org.apache.atlas.plugin.util.RangerCommonConstants.SCRIPT_FIELD_REMOTE_IP_ADDRESS;
import static org.apache.atlas.plugin.util.RangerCommonConstants.SCRIPT_FIELD_REQUEST;
import static org.apache.atlas.plugin.util.RangerCommonConstants.SCRIPT_FIELD_REQUEST_DATA;
import static org.apache.atlas.plugin.util.RangerCommonConstants.SCRIPT_FIELD_RESOURCE;
import static org.apache.atlas.plugin.util.RangerCommonConstants.SCRIPT_FIELD_RESOURCE_MATCHING_SCOPE;
import static org.apache.atlas.plugin.util.RangerCommonConstants.SCRIPT_FIELD_RESOURCE_OWNER_USER;
import static org.apache.atlas.plugin.util.RangerCommonConstants.SCRIPT_FIELD_TAG;
import static org.apache.atlas.plugin.util.RangerCommonConstants.SCRIPT_FIELD_TAGS;
import static org.apache.atlas.plugin.util.RangerCommonConstants.SCRIPT_FIELD_USER;
import static org.apache.atlas.plugin.util.RangerCommonConstants.SCRIPT_FIELD_USER_ATTRIBUTES;
import static org.apache.atlas.plugin.util.RangerCommonConstants.SCRIPT_FIELD_USER_GROUPS;
import static org.apache.atlas.plugin.util.RangerCommonConstants.SCRIPT_FIELD_USER_GROUP_ATTRIBUTES;
import static org.apache.atlas.plugin.util.RangerCommonConstants.SCRIPT_FIELD_USER_ROLES;


public final class RangerScriptExecutionContext {
	private static final Log LOG = LogFactory.getLog(RangerScriptExecutionContext.class);

	private static final Log    PERF_POLICY_CONDITION_SCRIPT_TOJSON         = RangerPerfTracer.getPerfLogger("policy.condition.script.tojson");
	private static final String TAG_ATTR_DATE_FORMAT_PROP                   = "ranger.plugin.tag.attr.additional.date.formats";
	private static final String TAG_ATTR_DATE_FORMAT_SEPARATOR              = "||";
	private static final String TAG_ATTR_DATE_FORMAT_SEPARATOR_REGEX        = "\\|\\|";
	private static final String DEFAULT_RANGER_TAG_ATTRIBUTE_DATE_FORMAT    = "yyyy/MM/dd";
	private static final String DEFAULT_ATLAS_TAG_ATTRIBUTE_DATE_FORMAT_NAME = "ATLAS_DATE_FORMAT";
	private static final String DEFAULT_ATLAS_TAG_ATTRIBUTE_DATE_FORMAT     = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

	private final RangerAccessRequest accessRequest;
	private Boolean result = false;

	private static String[] dateFormatStrings = null;

	static {
		init(null);
	}

	private static final ThreadLocal<List<SimpleDateFormat>> THREADLOCAL_DATE_FORMATS =
			new ThreadLocal<List<SimpleDateFormat>>() {
				@Override protected List<SimpleDateFormat> initialValue() {
					List<SimpleDateFormat> ret = new ArrayList<>();

					for (String dateFormatString : dateFormatStrings) {
						try {
							if (StringUtils.isNotBlank(dateFormatString)) {
								if (StringUtils.equalsIgnoreCase(dateFormatString, DEFAULT_ATLAS_TAG_ATTRIBUTE_DATE_FORMAT_NAME)) {
									dateFormatString = DEFAULT_ATLAS_TAG_ATTRIBUTE_DATE_FORMAT;
								}
								SimpleDateFormat df = new SimpleDateFormat(dateFormatString);
								df.setLenient(false);
								ret.add(df);
							}
						} catch (Exception exception) {
							// Ignore
						}
					}

					return ret;
				}
			};

	RangerScriptExecutionContext(final RangerAccessRequest accessRequest) {
		this.accessRequest = accessRequest;
	}

	public String toJson() {
		RangerPerfTracer perf = null;

		long requestHash = accessRequest.hashCode();

		if (RangerPerfTracer.isPerfTraceEnabled(PERF_POLICY_CONDITION_SCRIPT_TOJSON)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_POLICY_CONDITION_SCRIPT_TOJSON, "RangerScriptExecutionContext.toJson(requestHash=" + requestHash + ")");
		}

		Map<String, Object> ret       = new HashMap<>();
		Map<String, Object> request   = new HashMap<>();
		RangerUserStore     userStore = RangerAccessRequestUtil.getRequestUserStoreFromContext(accessRequest.getContext());

		if (accessRequest.getAccessTime() != null) {
			request.put(SCRIPT_FIELD_ACCESS_TIME, accessRequest.getAccessTime().getTime());
		}

		request.put(SCRIPT_FIELD_ACCESS_TYPE, accessRequest.getAccessType());
		request.put(SCRIPT_FIELD_ACTION, accessRequest.getAction());
		request.put(SCRIPT_FIELD_CLIENT_IP_ADDRESS, accessRequest.getClientIPAddress());
		request.put(SCRIPT_FIELD_CLIENT_TYPE, accessRequest.getClientType());
		request.put(SCRIPT_FIELD_CLUSTER_NAME, accessRequest.getClusterName());
		request.put(SCRIPT_FIELD_CLUSTER_TYPE, accessRequest.getClusterType());
		request.put(SCRIPT_FIELD_FORWARDED_ADDRESSES, accessRequest.getForwardedAddresses());
		request.put(SCRIPT_FIELD_REMOTE_IP_ADDRESS, accessRequest.getRemoteIPAddress());
		request.put(SCRIPT_FIELD_REQUEST_DATA, accessRequest.getRequestData());

		if (accessRequest.getResource() != null) {
			request.put(SCRIPT_FIELD_RESOURCE, accessRequest.getResource().getAsMap());
			request.put(SCRIPT_FIELD_RESOURCE_OWNER_USER, accessRequest.getResource().getOwnerUser());
		}

		request.put(SCRIPT_FIELD_RESOURCE_MATCHING_SCOPE, accessRequest.getResourceMatchingScope());

		request.put(SCRIPT_FIELD_USER, accessRequest.getUser());
		request.put(SCRIPT_FIELD_USER_GROUPS, accessRequest.getUserGroups());
		request.put(SCRIPT_FIELD_USER_ROLES, accessRequest.getUserRoles());

		if (userStore != null) {
			Map<String, Map<String, String>> userAttrMapping  = userStore.getUserAttrMapping();
			Map<String, Map<String, String>> groupAttrMapping = userStore.getGroupAttrMapping();

			if (userAttrMapping != null) {
				request.put(SCRIPT_FIELD_USER_ATTRIBUTES, userAttrMapping.get(accessRequest.getUser()));
			}

			if (groupAttrMapping != null && accessRequest.getUserGroups() != null) {
				Map<String, Map<String, String>> groupAttrs = new HashMap<>();

				for (String groupName : accessRequest.getUserGroups()) {
					groupAttrs.put(groupName, groupAttrMapping.get(groupName));
				}

				request.put(SCRIPT_FIELD_USER_GROUP_ATTRIBUTES, groupAttrs);
			}
		}

		ret.put(SCRIPT_FIELD_REQUEST, request);

		Set<RangerTagForEval> requestTags = RangerAccessRequestUtil.getRequestTagsFromContext(getRequestContext());

		if (CollectionUtils.isNotEmpty(requestTags)) {
			Set<Map<String, Object>> tags = new HashSet<>();

			for (RangerTagForEval tag : requestTags) {
				tags.add(toMap(tag));
			}

			ret.put(SCRIPT_FIELD_TAGS, tags);

			RangerTagForEval currentTag = RangerAccessRequestUtil.getCurrentTagFromContext(getRequestContext());

			if (currentTag != null) {
				ret.put(SCRIPT_FIELD_TAG, toMap(currentTag));
			}
		}

		RangerAccessResource resource = RangerAccessRequestUtil.getCurrentResourceFromContext(getRequestContext());

		if (resource != null) {
			ret.put(SCRIPT_FIELD_RESOURCE, resource.getAsMap());

			if (resource.getOwnerUser() != null) {
				ret.put(SCRIPT_FIELD_RESOURCE_OWNER_USER, resource.getOwnerUser());
			}
		}

		String strRet = JsonUtils.objectToJson(ret);

		RangerPerfTracer.log(perf);

		return strRet;
	}

	public static void init(Configuration config) {
		StringBuilder sb = new StringBuilder(DEFAULT_RANGER_TAG_ATTRIBUTE_DATE_FORMAT);

		sb.append(TAG_ATTR_DATE_FORMAT_SEPARATOR).append(DEFAULT_ATLAS_TAG_ATTRIBUTE_DATE_FORMAT_NAME);

		String additionalDateFormatsValue = config != null ? config.get(TAG_ATTR_DATE_FORMAT_PROP) : null;

		if (StringUtils.isNotBlank(additionalDateFormatsValue)) {
			sb.append(TAG_ATTR_DATE_FORMAT_SEPARATOR).append(additionalDateFormatsValue);
		}

		String[] formatStrings = sb.toString().split(TAG_ATTR_DATE_FORMAT_SEPARATOR_REGEX);

		Arrays.sort(formatStrings, new Comparator<String>() {
			@Override
			public int compare(String first, String second) {
				return Integer.compare(second.length(), first.length());
			}
		});

		RangerScriptExecutionContext.dateFormatStrings = formatStrings;
	}

	public String getResource() {
		String ret = null;
		RangerAccessResource val = RangerAccessRequestUtil.getCurrentResourceFromContext(getRequestContext());

		if(val != null) {
			ret = val.getAsString();
		}

		return ret;
	}

	public Map<String, Object> getRequestContext() {
		return accessRequest.getContext();
	}

	public String getRequestContextAttribute(String attributeName) {
		String ret = null;

		if (StringUtils.isNotBlank(attributeName)) {
			Object val = getRequestContext().get(attributeName);

			if(val != null) {
				ret = val.toString();
			}
		}

		return ret;
	}

	public boolean isAccessTypeAny() { return accessRequest.isAccessTypeAny(); }

	public boolean isAccessTypeDelegatedAdmin() { return accessRequest.isAccessTypeDelegatedAdmin(); }

	public String getUser() { return accessRequest.getUser(); }

	public Set<String> getUserGroups() { return accessRequest.getUserGroups(); }

	public Date getAccessTime() { return accessRequest.getAccessTime() != null ? accessRequest.getAccessTime() : new Date(); }

	public String getClientIPAddress() { return accessRequest.getClientIPAddress(); }

	public String getClientType() { return accessRequest.getClientType(); }

	public String getAction() { return accessRequest.getAction(); }

	public String getRequestData() { return accessRequest.getRequestData(); }

	public String getSessionId() { return accessRequest.getSessionId(); }

	public RangerTagForEval getCurrentTag() {
		RangerTagForEval ret = RangerAccessRequestUtil.getCurrentTagFromContext(getRequestContext());

		if(ret == null ) {
			if (LOG.isDebugEnabled()) {
				logDebug("RangerScriptExecutionContext.getCurrentTag() - No current TAG object. Script execution must be for resource-based policy.");
			}
		}
		return ret;
	}

	public String getCurrentTagType() {
		RangerTagForEval tagObject = getCurrentTag();
		return (tagObject != null) ? tagObject.getType() : null;
	}

	public Set<String> getAllTagTypes() {
		Set<String>     allTagTypes   = null;
		Set<RangerTagForEval> tagObjectList = getAllTags();

		if (CollectionUtils.isNotEmpty(tagObjectList)) {
			for (RangerTagForEval tag : tagObjectList) {
				String tagType = tag.getType();
				if (allTagTypes == null) {
					allTagTypes = new HashSet<>();
				}
				allTagTypes.add(tagType);
			}
		}

		return allTagTypes;
	}

	public Map<String, String> getTagAttributes(final String tagType) {
		Map<String, String> ret = null;

		if (StringUtils.isNotBlank(tagType)) {
			Set<RangerTagForEval> tagObjectList = getAllTags();

			// Assumption: There is exactly one tag with given tagType in the list of tags - may not be true ***TODO***
			// This will get attributes of the first tagType that matches
			if (CollectionUtils.isNotEmpty(tagObjectList)) {
				for (RangerTagForEval tag : tagObjectList) {
					if (tag.getType().equals(tagType)) {
						ret = tag.getAttributes();
						break;
					}
				}
			}
		}

		return ret;
	}

	public List<Map<String, String>> getTagAttributesForAllMatchingTags(final String tagType) {
		List<Map<String, String>> ret = null;

		if (StringUtils.isNotBlank(tagType)) {
			Set<RangerTagForEval> tagObjectList = getAllTags();

			// Assumption: There is exactly one tag with given tagType in the list of tags - may not be true ***TODO***
			// This will get attributes of the first tagType that matches
			if (CollectionUtils.isNotEmpty(tagObjectList)) {
				for (RangerTagForEval tag : tagObjectList) {
					if (tag.getType().equals(tagType)) {
						Map<String, String> tagAttributes = tag.getAttributes();
						if (tagAttributes != null) {
							if (ret == null) {
								ret = new ArrayList<>();
							}
							ret.add(tagAttributes);
						}
						break;
					}
				}
			}
		}

		return ret;
	}

	public Set<String> getAttributeNames(final String tagType) {
		Set<String>         ret        = null;
		Map<String, String> attributes = getTagAttributes(tagType);

		if (attributes != null) {
			ret = attributes.keySet();
		}

		return ret;
	}

	public String getAttributeValue(final String tagType, final String attributeName) {
		String ret = null;

		if (StringUtils.isNotBlank(tagType) || StringUtils.isNotBlank(attributeName)) {
			Map<String, String> attributes = getTagAttributes(tagType);

			if (attributes != null) {
				ret = attributes.get(attributeName);
			}
		}
		return ret;
	}

	public List<String> getAttributeValueForAllMatchingTags(final String tagType, final String attributeName) {
		List<String> ret = null;

		if (StringUtils.isNotBlank(tagType) || StringUtils.isNotBlank(attributeName)) {
			Map<String, String> attributes = getTagAttributes(tagType);

			if (attributes != null && attributes.get(attributeName) != null) {
				if (ret == null) {
					ret = new ArrayList<>();
				}
				ret.add(attributes.get(attributeName));
			}
		}
		return ret;
	}

	public String getAttributeValue(final String attributeName) {
		String ret = null;

		if (StringUtils.isNotBlank(attributeName)) {
			RangerTagForEval tag = getCurrentTag();
			Map<String, String> attributes = null;
			if (tag != null) {
				attributes = tag.getAttributes();
			}
			if (attributes != null) {
				ret = attributes.get(attributeName);
			}
		}

		return ret;
	}

	public boolean getResult() {
		return result;

	}

	public void setResult(final boolean result) {
		this.result = result;
	}

	private Date getAsDate(String value, SimpleDateFormat df) {
		Date ret = null;

		TimeZone savedTimeZone = df.getTimeZone();
		try {
			ret = df.parse(value);
		} catch (ParseException exception) {
			// Ignore
		} finally {
			df.setTimeZone(savedTimeZone);
		}

		return ret;
	}

	public Date getAsDate(String value) {
		Date ret = null;

		if (StringUtils.isNotBlank(value)) {
			for (SimpleDateFormat simpleDateFormat : THREADLOCAL_DATE_FORMATS.get()) {
				ret = getAsDate(value, simpleDateFormat);
				if (ret != null) {
					if (LOG.isDebugEnabled()) {
						logDebug("RangerScriptExecutionContext.getAsDate() -The best match found for Format-String:[" + simpleDateFormat.toPattern() + "], date:[" + ret +"]");
					}
					break;
				}
			}
		}

		if (ret == null) {
			logError("RangerScriptExecutionContext.getAsDate() - Could not convert [" + value + "] to Date using any of the Format-Strings: " + Arrays.toString(dateFormatStrings));
		} else {
			ret = StringUtil.getUTCDateForLocalDate(ret);
		}

		return ret;
	}

	public Date getTagAttributeAsDate(String tagType, String attributeName) {
		String attrValue = getAttributeValue(tagType, attributeName);

		return getAsDate(attrValue);
	}

	public boolean isAccessedAfter(String tagType, String attributeName) {
		boolean ret        = false;
		Date    accessDate = getAccessTime();
		Date    expiryDate = getTagAttributeAsDate(tagType, attributeName);

		if (expiryDate == null || accessDate.after(expiryDate) || accessDate.equals(expiryDate)) {
			ret = true;
		}

		return ret;
	}

	public boolean isAccessedAfter(String attributeName) {
		boolean ret        = false;
		Date    accessDate = getAccessTime();
		Date    expiryDate = getAsDate(getAttributeValue(attributeName));

		if (expiryDate == null || accessDate.after(expiryDate) || accessDate.equals(expiryDate)) {
			ret = true;
		}

		return ret;
	}

	public boolean isAccessedBefore(String tagType, String attributeName) {
		boolean ret        = true;
		Date    accessDate = getAccessTime();
		Date    expiryDate = getTagAttributeAsDate(tagType, attributeName);

		if (expiryDate == null || accessDate.after(expiryDate)) {
			ret = false;
		}

		return ret;
	}

	public boolean isAccessedBefore(String attributeName) {
		boolean ret        = true;
		Date    accessDate = getAccessTime();
		Date    expiryDate = getAsDate(getAttributeValue(attributeName));

		if (expiryDate == null || accessDate.after(expiryDate)) {
			ret = false;
		}

		return ret;
	}

	private Set<RangerTagForEval> getAllTags() {
		Set<RangerTagForEval> ret = RangerAccessRequestUtil.getRequestTagsFromContext(accessRequest.getContext());
		if(ret == null) {
			if (LOG.isDebugEnabled()) {
				String resource = accessRequest.getResource().getAsString();

				logDebug("RangerScriptExecutionContext.getAllTags() - No TAGS. No TAGS for the RangerAccessResource=" + resource);
			}
		}

		return ret;
	}

	private static Map<String, Object> toMap(RangerTagForEval tag) {
		Map<String, Object> ret = new HashMap<>();

		ret.put("type", tag.getType());
		ret.put("attributes", tag.getAttributes());
		ret.put("matchType", tag.getMatchType());

		return ret;
	}

	public void logDebug(Object msg) {
		LOG.debug(msg);
	}

	public void logInfo(Object msg) {
		LOG.info(msg);
	}

	public void logWarn(Object msg) {
		LOG.warn(msg);
	}

	public void logError(Object msg) {
		LOG.error(msg);
	}

	public void logFatal(Object msg) {
		LOG.fatal(msg);
	}
}
