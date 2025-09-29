package org.apache.atlas.services;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.service.FeatureFlag;
import org.apache.atlas.service.FeatureFlagStore;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.util.List;

/**
 * Automatically enables Tag V2 for new Atlas instances
 * where no classification types are present. This ensures new customer tenants get the
 * latest tag propagation version by default.
 */
@Component
public class TagsV2AutoEnabler {
    private static final Logger LOG = LoggerFactory.getLogger(TagsV2AutoEnabler.class);

    private static final String CLASSIFICATION_TYPE = "classification";
    private static final String ENABLE_JANUS_OPTIMISATION_KEY = FeatureFlag.ENABLE_JANUS_OPTIMISATION.getKey();

    private final AtlasTypeDefStore typeDefStore;

    @Inject
    public TagsV2AutoEnabler(AtlasTypeDefStore typeDefStore) {
        this.typeDefStore = typeDefStore;
    }

    @PostConstruct
    public void checkAndEnableJanusOptimisation() throws AtlasBaseException {
        try {
            LOG.info("Starting auto-enable check for Janus optimization...");

            boolean isTagV2Enabled = FeatureFlagStore.isTagV2Enabled();
            if (isTagV2Enabled) {
                LOG.info("Tags v2 optimization is already enabled, skipping auto-enable check");
                return;
            }

            // Check if there are any classification types present
            boolean hasClassificationTypes = hasClassificationTypes();

            if (!hasClassificationTypes) {
                LOG.info("No classification types found - enabling Janus optimization for new tenant");
                FeatureFlagStore.setFlag(ENABLE_JANUS_OPTIMISATION_KEY, "true");
                LOG.info("Successfully enabled Janus optimization feature flag");
            } else {
                LOG.info("Classification types found - keeping existing configuration (Janus optimization disabled)");
            }

        } catch (Exception e) {
            LOG.error("Error during auto-enable check for Tags v2 optimization", e);
            throw e;
        }
    }

    /**
     * Checks if there are any classification types present in the system
     * @return true if classification types exist, false otherwise
     */
    private boolean hasClassificationTypes() throws AtlasBaseException {
        try {
            SearchFilter searchFilter = new SearchFilter();
            searchFilter.setParam(SearchFilter.PARAM_TYPE, List.of(CLASSIFICATION_TYPE));
            AtlasTypesDef typesDef = typeDefStore.searchTypesDef(searchFilter);

            boolean hasClassifications = typesDef != null && !CollectionUtils.isEmpty(typesDef.getClassificationDefs());

            LOG.info("Found {} classification types", hasClassifications ? typesDef.getClassificationDefs().size() : 0);
            return hasClassifications;
        } catch (AtlasBaseException e) {
            LOG.error("Error checking for classification types", e);
            throw e;
        }
    }
}