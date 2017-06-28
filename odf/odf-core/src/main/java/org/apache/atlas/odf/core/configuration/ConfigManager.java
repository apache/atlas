/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.odf.core.configuration;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.analysis.AnalysisRequest;
import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.api.settings.SparkConfig;
import org.apache.atlas.odf.api.settings.validation.ValidationException;
import org.apache.atlas.odf.core.Encryption;
import org.apache.atlas.odf.core.Environment;
import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.json.JSONUtils;
import org.apache.wink.json4j.JSONException;

import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.core.Utils;
import org.apache.atlas.odf.core.controlcenter.ControlCenter;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceProperties;
import org.apache.atlas.odf.core.store.ODFConfigurationStorage;

public class ConfigManager {
	private Logger logger = Logger.getLogger(ConfigManager.class.getName());
	public static final String HIDDEN_PASSWORD_IDENTIFIER = "***hidden***";
	public static final long CONFIG_UPDATE_SLEEP_BETWEEN_POLLS = 20;
	public static final int CONFIG_UPDATE_MAX_POLLS = 1500;
	private static final String DEFAULT_ENCRYPTED_SPARK_CONFIGS = "spark.authenticate.secret,spark.ssl.keyPassword,spark.ssl.keyStorePassword,spark.ssl.trustStorePassword";

	protected ODFConfigurationStorage configurationStore;
	protected ODFConfigNotificationPublisher notificationManager;

	public ConfigManager() {
		ODFInternalFactory f = new ODFInternalFactory();
		this.configurationStore = f.create(ODFConfigurationStorage.class);
		this.notificationManager = f.create(ODFConfigNotificationPublisher.class);
	}

	public ConfigContainer getConfigContainer() {
		ConfigContainer config = configurationStore.getConfig(getDefaultConfigContainer());
		return config;
	}

	public ConfigContainer getConfigContainerHidePasswords() {
		ConfigContainer config = configurationStore.getConfig(getDefaultConfigContainer());
		hidePasswords(config);
		return config;
	}

	public void updateConfigContainer(ConfigContainer update) throws ValidationException {
		try {
			update = JSONUtils.cloneJSONObject(update);
		} catch (JSONException e) {
			throw new RuntimeException(e);
		}
		update.validate();
		ConfigContainer source = getConfigContainer();
		unhideAndEncryptPasswords(update, source);

		List<DiscoveryServiceProperties> newServicesToRun = new ArrayList<DiscoveryServiceProperties>();
		if (update.getRegisteredServices() != null
				&& source.getRegisteredServices().size() < update.getRegisteredServices().size()) {
			// store added services if update registers new ones
			List<DiscoveryServiceProperties> newRegisteredServices = new ArrayList<DiscoveryServiceProperties>();
			newRegisteredServices.addAll(update.getRegisteredServices());
			for (DiscoveryServiceProperties oldService : source.getRegisteredServices()) {
				for (int no = 0; no < newRegisteredServices.size(); no++) {
					if (newRegisteredServices.get(no).getId().equals(oldService.getId())) {
						newRegisteredServices.remove(no);
						break;
					}
				}
			}

			newServicesToRun.addAll(newRegisteredServices);
		}

		Utils.mergeODFPOJOs(source, update);
		configurationStore.storeConfig(source);

		if (source.getOdf().getRunNewServicesOnRegistration() && !newServicesToRun.isEmpty()) {
			runNewServices(newServicesToRun);
		}

		String changeId = UUID.randomUUID().toString();
		configurationStore.addPendingConfigChange(changeId);
		this.notificationManager.publishConfigChange(source, changeId);
		for (int i=0; i < CONFIG_UPDATE_MAX_POLLS; i++) {
			if (!configurationStore.isConfigChangePending(changeId)) {
				logger.log(Level.INFO, MessageFormat.format("Config change id ''{0}'' successfully completed after {1} msec.", new Object[] { changeId, i * CONFIG_UPDATE_SLEEP_BETWEEN_POLLS } ));
				return;
			}
			try {
				Thread.sleep(CONFIG_UPDATE_SLEEP_BETWEEN_POLLS);
			} catch (InterruptedException e) {
				// Ignore interrupt
				logger.log(Level.WARNING, "Sleep period was interrupted", e);
			}
		}
		logger.log(Level.WARNING, MessageFormat.format("Config change did not complete after {0} msec.", CONFIG_UPDATE_SLEEP_BETWEEN_POLLS * CONFIG_UPDATE_MAX_POLLS));
	}

	public void resetConfigContainer() {
		logger.warning("resetting ODF configuration!");
		configurationStore.storeConfig(getDefaultConfigContainer());
	}

	private static String defaultConfig = null;

	List<DiscoveryServiceProperties> getServicesFoundOnClassPath() throws IOException, JSONException {
		ClassLoader cl = this.getClass().getClassLoader();
		Enumeration<URL> services = cl.getResources("META-INF/odf/odf-services.json");
		List<DiscoveryServiceProperties> result = new ArrayList<>();
		while (services.hasMoreElements()) {
			URL url = services.nextElement();
			InputStream is = url.openStream();
			String json = Utils.getInputStreamAsString(is, "UTF-8");
			logger.log(Level.INFO, "Service found on the classpath at {0}: {1}", new Object[] { url, json });
			result.addAll(JSONUtils.fromJSONList(json, DiscoveryServiceProperties.class));
		}
		logger.log(Level.INFO, "Number of classpath services found: {0}", result.size());
		return result;
	}

	private ConfigContainer getDefaultConfigContainer() {
		if (defaultConfig == null) {			
			try {
				ConfigContainer config = new ODFInternalFactory().create(Environment.class).getDefaultConfiguration();
				// now look for services found on the classpath
				config.getRegisteredServices().addAll(getServicesFoundOnClassPath());
				defaultConfig = JSONUtils.toJSON(config);
			} catch (IOException | JSONException e) {
				String msg = "Default config could not be loaded or parsed!";
				logger.severe(msg);
				throw new RuntimeException(msg, e);
			}
		}
		try {
			return JSONUtils.fromJSON(defaultConfig, ConfigContainer.class);
		} catch (JSONException e) {
			throw new RuntimeException(e);
		}
	}

	private void runNewServices(List<DiscoveryServiceProperties> newServices) {
		ControlCenter cc = new ODFInternalFactory().create(ControlCenter.class);
		List<String> servicesToRun = new ArrayList<String>();
		for (DiscoveryServiceProperties info : newServices) {
			servicesToRun.add(info.getId());
		}

		AnalysisRequest req = new AnalysisRequest();
		MetadataStore mds = new ODFFactory().create().getMetadataStore();
		req.setDiscoveryServiceSequence(servicesToRun);
		req.setDataSets(mds.search(mds.newQueryBuilder().objectType("DataSet").build()));
		req.setIgnoreDataSetCheck(true);
		cc.startRequest(req);
	}

	private void unhideAndEncryptPasswords(ConfigContainer updatedConfigContainer,
			ConfigContainer originalConfiguration) {
		if (updatedConfigContainer.getOdf() != null) {
			String odfPassword = updatedConfigContainer.getOdf().getOdfPassword();
			if (odfPassword != null) {
				if (odfPassword.equals(HIDDEN_PASSWORD_IDENTIFIER)) {
					// Password was not changed, therefore keep original
					// encrypted password
					updatedConfigContainer.getOdf().setOdfPassword(originalConfiguration.getOdf().getOdfPassword());
				} else if (!Encryption.isEncrypted(odfPassword)) {
					updatedConfigContainer.getOdf().setOdfPassword(Encryption.encryptText(odfPassword));
				}
			}
			if (updatedConfigContainer.getOdf().getSparkConfig() != null) {
				SparkConfig updatedSparkConfig = updatedConfigContainer.getOdf().getSparkConfig();
				if (updatedSparkConfig.getConfigs() != null) {
					List<String> encryptedSparkConfigs = Arrays.asList(DEFAULT_ENCRYPTED_SPARK_CONFIGS.split(","));
					for (String configName : updatedSparkConfig.getConfigs().keySet()) {
						if (encryptedSparkConfigs.contains(configName)) {
							String updatedConfigValue = (String) updatedSparkConfig.getConfigs().get(configName);
							if (updatedConfigValue.equals(HIDDEN_PASSWORD_IDENTIFIER)) {
								// Encrypted value was not changed, therefore keep original
								// Encrypted value
								SparkConfig originalSparkConfig = originalConfiguration.getOdf().getSparkConfig();
								updatedSparkConfig.setConfig(configName, originalSparkConfig.getConfigs().get(configName));
							} else if (!Encryption.isEncrypted(updatedConfigValue)) {
								updatedSparkConfig.setConfig(configName, Encryption.encryptText(updatedConfigValue));
							}
						}
					}
				}
			}
		}
	}

	private void hidePasswords(ConfigContainer configContainer) {
		if (configContainer.getOdf() != null) {
			if (configContainer.getOdf().getOdfPassword() != null) {
				configContainer.getOdf().setOdfPassword(HIDDEN_PASSWORD_IDENTIFIER);
			}
			if ((configContainer.getOdf().getSparkConfig() != null)){
				SparkConfig sparkConfig = configContainer.getOdf().getSparkConfig();
				if (sparkConfig.getConfigs() != null) {
					List<String> encryptedSparkConfigs = Arrays.asList(DEFAULT_ENCRYPTED_SPARK_CONFIGS.split(","));
					for (String configName : sparkConfig.getConfigs().keySet()) {
						if (((encryptedSparkConfigs.contains(configName)) && (sparkConfig.getConfigs().get(configName)) != null)) {
							sparkConfig.setConfig(configName, HIDDEN_PASSWORD_IDENTIFIER);
						}
					}
				}
			}
		}
	}

}
