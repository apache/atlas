/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.audit.provider;

import org.apache.atlas.audit.utils.AuthObjectUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetAddress;
import java.rmi.dgc.VMID;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.UUID;


public class MiscUtil {
	private static final Logger logger = LoggerFactory.getLogger(MiscUtil.class);

	public static final String TOKEN_START = "%";
	public static final String TOKEN_END = "%";
	public static final String TOKEN_HOSTNAME = "hostname";
	public static final String TOKEN_APP_TYPE = "app-type";
	public static final String TOKEN_JVM_INSTANCE = "jvm-instance";
	public static final String TOKEN_TIME = "time:";
	public static final String TOKEN_PROPERTY = "property:";
	public static final String TOKEN_ENV = "env:";

	private static final VMID         sJvmID        = new VMID();

	private static String sApplicationType = null;
	private static String local_hostname = null;

	private static Map<String, LogHistory> logHistoryList = new Hashtable<String, LogHistory>();
	private static int logInterval = 30000; // 30 seconds

	public static String replaceTokens(String str, long time) {
		if (str == null) {
			return str;
		}

		if (time <= 0) {
			time = System.currentTimeMillis();
		}

		for (int startPos = 0; startPos < str.length();) {
			int tagStartPos = str.indexOf(TOKEN_START, startPos);

			if (tagStartPos == -1) {
				break;
			}

			int tagEndPos = str.indexOf(TOKEN_END,
					tagStartPos + TOKEN_START.length());

			if (tagEndPos == -1) {
				break;
			}

			String tag = str.substring(tagStartPos,
					tagEndPos + TOKEN_END.length());
			String token = tag.substring(TOKEN_START.length(),
					tag.lastIndexOf(TOKEN_END));
			String val = "";

			if (token != null) {
				if (token.equals(TOKEN_HOSTNAME)) {
					val = getHostname();
				} else if (token.equals(TOKEN_APP_TYPE)) {
					val = getApplicationType();
				} else if (token.equals(TOKEN_JVM_INSTANCE)) {
					val = getJvmInstanceId();
				} else if (token.startsWith(TOKEN_PROPERTY)) {
					String propertyName = token.substring(TOKEN_PROPERTY
							.length());

					val = getSystemProperty(propertyName);
				} else if (token.startsWith(TOKEN_ENV)) {
					String envName = token.substring(TOKEN_ENV.length());

					val = getEnv(envName);
				} else if (token.startsWith(TOKEN_TIME)) {
					String dtFormat = token.substring(TOKEN_TIME.length());

					val = getFormattedTime(time, dtFormat);
				}
			}

			if (val == null) {
				val = "";
			}

			str = str.substring(0, tagStartPos) + val
					+ str.substring(tagEndPos + TOKEN_END.length());
			startPos = tagStartPos + val.length();
		}

		return str;
	}

	public static String getHostname() {
		String ret = local_hostname;

		if  (ret == null) {
			initLocalHost();

			ret = local_hostname;

			if (ret == null) {
				ret = "unknown";
			}
		}

		return ret;
	}

	public static void setApplicationType(String applicationType) {
		sApplicationType = applicationType;
	}

	public static String getApplicationType() {
		return sApplicationType;
	}

	public static String getJvmInstanceId() {
		Integer val  = Integer.valueOf(sJvmID.toString().hashCode());
		long longVal = val.longValue();
		String ret 	 = Long.toString(Math.abs(longVal));

		return ret;
	}

	public static String getSystemProperty(String propertyName) {
		String ret = null;

		try {
			ret = propertyName != null ? System.getProperty(propertyName)
					: null;
		} catch (Exception excp) {
			logger.warn("getSystemProperty(" + propertyName + ") failed", excp);
		}

		return ret;
	}

	public static String getEnv(String envName) {
		String ret = null;

		try {
			ret = envName != null ? System.getenv(envName) : null;
		} catch (Exception excp) {
			logger.warn("getenv(" + envName + ") failed", excp);
		}

		return ret;
	}

	public static String getFormattedTime(long time, String format) {
		String ret = null;

		try {
			SimpleDateFormat sdf = new SimpleDateFormat(format);

			ret = sdf.format(time);
		} catch (Exception excp) {
			logger.warn("SimpleDateFormat.format() failed: " + format, excp);
		}

		return ret;
	}

	public static void createParents(File file) {
		if (file != null) {
			String parentName = file.getParent();

			if (parentName != null) {
				File parentDir = new File(parentName);

				if (!parentDir.exists()) {
					if (!parentDir.mkdirs()) {
						logger.warn("createParents(): failed to create "
								+ parentDir.getAbsolutePath());
					}
				}
			}
		}
	}

	public static long getNextRolloverTime(long lastRolloverTime, long interval) {
		long now = System.currentTimeMillis() / 1000 * 1000; // round to second

		if (lastRolloverTime <= 0) {
			// should this be set to the next multiple-of-the-interval from
			// start of the day?
			return now + interval;
		} else if (lastRolloverTime <= now) {
			long nextRolloverTime = now + interval;

			// keep it at 'interval' boundary
			long trimInterval = (nextRolloverTime - lastRolloverTime)
					% interval;

			return nextRolloverTime - trimInterval;
		} else {
			return lastRolloverTime;
		}
	}

	public static long getRolloverStartTime(long nextRolloverTime, long interval) {
		return (nextRolloverTime <= interval) ? System.currentTimeMillis()
				: nextRolloverTime - interval;
	}

	public static int parseInteger(String str, int defValue) {
		int ret = defValue;

		if (str != null) {
			try {
				ret = Integer.parseInt(str);
			} catch (Exception excp) {
				// ignore
			}
		}

		return ret;
	}

	public static String generateUniqueId() {
		return UUID.randomUUID().toString();
	}

	// UUID.randomUUID() uses SecureRandom, which is seen to be slow in some environments; this method uses Random
	public static String generateGuid() {
		byte[] randomBytes = new byte[16];

		RandomHolder.random.nextBytes(randomBytes);

		UUID uuid = UUID.nameUUIDFromBytes(randomBytes);

		return uuid.toString();
	}

	public static <T> String stringify(T log) {
		String ret = null;

		if (log != null) {
			if (log instanceof String) {
				ret = (String) log;
			} else {
				ret = AuthObjectUtil.toJson(log);
			}
		}

		return ret;
	}

	static public <T> T fromJson(String jsonStr, Class<T> clazz) {
		return AuthObjectUtil.fromJson(jsonStr, clazz);
	}

	public static String getStringProperty(Properties props, String propName) {
		String ret = null;

		if (props != null && propName != null) {
			String val = props.getProperty(propName);
			if (val != null) {
				ret = val;
			}
		}

		return ret;
	}

	public static String getStringProperty(Properties props, String propName, String defValue) {
		String ret = defValue;

		if (props != null && propName != null) {
			String val = props.getProperty(propName);
			if (val != null) {
				ret = val;
			}
		}

		return ret;
	}

	public static boolean getBooleanProperty(Properties props, String propName,
			boolean defValue) {
		boolean ret = defValue;

		if (props != null && propName != null) {
			String val = props.getProperty(propName);

			if (val != null) {
				ret = Boolean.valueOf(val);
			}
		}

		return ret;
	}

	public static int getIntProperty(Properties props, String propName,
			int defValue) {
		int ret = defValue;

		if (props != null && propName != null) {
			String val = props.getProperty(propName);
			if (val != null) {
				try {
					ret = Integer.parseInt(val);
				} catch (NumberFormatException excp) {
					ret = defValue;
				}
			}
		}

		return ret;
	}

	public static long getLongProperty(Properties props, String propName,
			long defValue) {
		long ret = defValue;

		if (props != null && propName != null) {
			String val = props.getProperty(propName);
			if (val != null) {
				try {
					ret = Long.parseLong(val);
				} catch (NumberFormatException excp) {
					ret = defValue;
				}
			}
		}

		return ret;
	}

	public static Map<String, String> getPropertiesWithPrefix(Properties props,
			String prefix) {
		Map<String, String> prefixedProperties = new HashMap<String, String>();

		if (props != null && prefix != null) {
			for (String key : props.stringPropertyNames()) {
				if (key == null) {
					continue;
				}

				String val = props.getProperty(key);

				if (key.startsWith(prefix)) {
					key = key.substring(prefix.length());

					if (key == null) {
						continue;
					}

					prefixedProperties.put(key, val);
				}
			}
		}

		return prefixedProperties;
	}

	/**
	 * @param destListStr
	 * @param delim
	 * @return
	 */
	public static List<String> toArray(String destListStr, String delim) {
		List<String> list = new ArrayList<String>();
		if (destListStr != null && !destListStr.isEmpty()) {
			StringTokenizer tokenizer = new StringTokenizer(destListStr,
					delim.trim());
			while (tokenizer.hasMoreTokens()) {
				list.add(tokenizer.nextToken());
			}
		}
		return list;
	}

	static public boolean logErrorMessageByInterval(Logger useLogger,
			String message) {
		return logErrorMessageByInterval(useLogger, message, null);
	}

	/**
	 * @param useLogger
	 * @param message
	 * @param e
	 */
	static public boolean logErrorMessageByInterval(Logger useLogger,
			String message, Throwable e) {
        if (message == null) {
            return false;
        }

		LogHistory log = logHistoryList.get(message);
		if (log == null) {
			log = new LogHistory();
			logHistoryList.put(message, log);
		}
		if ((System.currentTimeMillis() - log.lastLogTime) > logInterval) {
			log.lastLogTime = System.currentTimeMillis();
			int counter = log.counter;
			log.counter = 0;
			if (counter > 0) {
				message += ". Messages suppressed before: " + counter;
			}
			if (e == null) {
				useLogger.error(message);
			} else {
				useLogger.error(message, e);
			}

			return true;
		} else {
			log.counter++;
		}
		return false;

	}

	static class LogHistory {
		long lastLogTime = 0;
		int counter = 0;
	}

	private static void initLocalHost() {
		if ( logger.isDebugEnabled() ) {
			logger.debug("==> MiscUtil.initLocalHost()");
		}

		try {
			local_hostname = InetAddress.getLocalHost().getHostName();
		} catch (Throwable excp) {
			logger.warn("getHostname()", excp);
		}
		if ( logger.isDebugEnabled() ) {
			logger.debug("<== MiscUtil.initLocalHost()");
		}
	}

	// use Holder class to defer initialization until needed
	private static class RandomHolder {
		static final Random random = new Random();
	}

}
