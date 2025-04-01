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

 package org.apache.atlas.authorization.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.atlas.plugin.model.AuditFilter;
import org.apache.atlas.plugin.model.RangerValiditySchedule;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

public class RangerUtil {

	private static final Log LOG = LogFactory.getLog(RangerUtil.class);

    private static final TimeZone gmtTimeZone = TimeZone.getTimeZone("GMT+0");

	private static final ThreadLocal<Gson> gson = new ThreadLocal<Gson>() {
		@Override
		protected Gson initialValue() {
			return new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").create();
		}
	};

	public static String objectToJson(Object object) {
		String ret = null;

		if(object != null) {
			try {
				ret = gson.get().toJson(object);
			} catch(Exception excp) {
				LOG.warn("objectToJson() failed to convert object to Json", excp);
			}
		}

		return ret;
	}

	public static List<RangerValiditySchedule> jsonToRangerValiditySchedule(String jsonStr) {
		try {
			Type listType = new TypeToken<List<RangerValiditySchedule>>() {}.getType();
			return gson.get().fromJson(jsonStr, listType);
		} catch (Exception e) {
			LOG.error("Cannot get List<RangerValiditySchedule> from " + jsonStr, e);
			return null;
		}
	}

	public static List<AuditFilter> jsonToAuditFilterList(String jsonStr) {
		try {
			Type listType = new TypeToken<List<AuditFilter>>() {}.getType();
			return gson.get().fromJson(jsonStr, listType);
		} catch (Exception e) {
			LOG.error("failed to create audit filters from: " + jsonStr, e);
			return null;
		}
	}


	public static String toString(List<String> arr) {
		String ret = "";

		if(arr != null && !arr.isEmpty()) {
			ret = arr.get(0);
			for(int i = 1; i < arr.size(); i++) {
				ret += (", " + arr.get(i));
			}
		}
		
		return ret;
	}

	public static boolean isEmpty(String str) {
		return str == null || str.trim().isEmpty();
	}

	public static Date getUTCDateForLocalDate(Date date) {
		Calendar local  = Calendar.getInstance();
		int      offset = local.getTimeZone().getOffset(local.getTimeInMillis());

		GregorianCalendar utc = new GregorianCalendar(gmtTimeZone);

		utc.setTimeInMillis(date.getTime());
		utc.add(Calendar.MILLISECOND, -offset);

		return utc.getTime();
	}

	public static Map<String, Object> toStringObjectMap(Map<String, String> map) {
		Map<String, Object> ret = null;

		if (map != null) {
			ret = new HashMap<>(map.size());

			for (Map.Entry<String, String> e : map.entrySet()) {
				ret.put(e.getKey(), e.getValue());
			}
		}

		return ret;
	}

	public static Set<String> toSet(String str) {
		Set<String> values = new HashSet<String>();
		if (StringUtils.isNotBlank(str)) {
			for (String item : str.split(",")) {
				if (StringUtils.isNotBlank(item)) {
					values.add(StringUtils.trim(item));
				}
			}
		}
		return values;
	}

	public static List<String> toList(String str) {
		List<String> values;
		if (StringUtils.isNotBlank(str)) {
			values = new ArrayList<>();
			for (String item : str.split(",")) {
				if (StringUtils.isNotBlank(item)) {
					values.add(StringUtils.trim(item));
				}
			}
		} else {
			values = Collections.emptyList();
		}
		return values;
	}

	public static List<String> getURLs(String configURLs) {
		List<String> configuredURLs = new ArrayList<>();
		if(configURLs!=null) {
			String[] urls = configURLs.split(",");
			for (String strUrl : urls) {
				if (StringUtils.isNotEmpty(StringUtils.trimToEmpty(strUrl))) {
					if (strUrl.endsWith("/")) {
						strUrl = strUrl.substring(0, strUrl.length() - 1);
					}
					configuredURLs.add(strUrl);
				}
			}
		}
		return configuredURLs;
	}
}
