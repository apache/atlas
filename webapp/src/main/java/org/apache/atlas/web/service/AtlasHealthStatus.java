package org.apache.atlas.web.service;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.model.general.HealthStatus;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

@Component
public class AtlasHealthStatus {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasHealthStatus.class);
    private static final String ERROR = "error";
    private static final String OK = "ok";

    private final List<HealthStatus> healthStatuses = new ArrayList<>();
    private boolean isActiveActiveHAEnabled;

    @PostConstruct
    public void init() throws AtlasException {
        Configuration atlasProperties = ApplicationProperties.get();
        this.isActiveActiveHAEnabled = HAConfiguration.isActiveActiveHAEnabled(atlasProperties);
        registerComponents();
    }

    private void registerComponents() {
        //Initialize with OK status.
        if (isActiveActiveHAEnabled) {
            healthStatuses.add(new HealthStatus(Component.TYPE_DEF_CACHE.componentName, OK, false, new Date().toString(), StringUtils.EMPTY));
        }
    }

    public void markUnhealthy(final Component component, final String errorString) {
        HealthStatus health = getHealth(component);
        if (health != null) {
            health.status = ERROR;
            health.errorString = errorString;
            health.checkTime = new Date().toString();
        }
    }

    public HealthStatus getHealth(final Component component) {
        Optional<HealthStatus> optionalHealthStatus = healthStatuses.stream().filter(
                healthStatus -> healthStatus.name.equalsIgnoreCase(component.componentName)).findFirst();

        if (!optionalHealthStatus.isPresent()) {
            LOG.error("Could not find component {} in health status. Should have been registered first", component.componentName);
            return null;
        } else {
            return optionalHealthStatus.get();
        }
    }

    public List<HealthStatus> getHealthStatuses() {
        return healthStatuses;
    }

    public boolean isAtleastOneComponentUnHealthy() {
        return healthStatuses.stream().anyMatch(healthStatus -> healthStatus.status.equalsIgnoreCase(ERROR));
    }

    public enum Component {
        TYPE_DEF_CACHE("typeDefCache");

        private final String componentName;

        Component(String componentName) {
            this.componentName = componentName;
        }

    }
}