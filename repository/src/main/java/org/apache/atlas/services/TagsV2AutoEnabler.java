package org.apache.atlas.services;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.service.FeatureFlag;
import org.apache.atlas.service.FeatureFlagStore;
import org.apache.atlas.typesystem.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.util.Iterator;


/**
 * Automatically enables Tag V2 for new Atlas instances
 * where no classification types are present. This ensures new customer tenants get the
 * latest tag propagation version by default.
 */
@Component
@DependsOn("featureFlagStore")
public class TagsV2AutoEnabler {
    private static final Logger LOG = LoggerFactory.getLogger(TagsV2AutoEnabler.class);

    private static final String ENABLE_JANUS_OPTIMISATION_KEY = FeatureFlag.ENABLE_JANUS_OPTIMISATION.getKey();
    
    // Property keys for type system vertices
    private static final String VERTEX_TYPE_PROPERTY_KEY = Constants.VERTEX_TYPE_PROPERTY_KEY;
    private static final String TYPE_CATEGORY_PROPERTY_KEY = Constants.TYPE_CATEGORY_PROPERTY_KEY;
    private static final String VERTEX_TYPE = AtlasGraphUtilsV2.VERTEX_TYPE; // "typeSystem"

    private final AtlasGraph graph;

    @Inject
    public TagsV2AutoEnabler(AtlasGraph graph) {
        this.graph = graph;
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
                LOG.info("No classification types found - enabling Tags V2 for new tenant");
                FeatureFlagStore.setFlag(ENABLE_JANUS_OPTIMISATION_KEY, "true");
                LOG.info("Successfully enabled Tags V2 feature flag");
            } else {
                LOG.info("Classification types found - keeping existing configuration (Tags v2 disabled)");
            }

        } catch (Exception e) {
            LOG.error("Error during auto-enable check for Tags v2 optimization", e);
            throw e;
        }
    }

    /**
     * Optimized method to check if any classification types exist
     * Uses direct graph query instead of loading all typedefs
     * @return true if classification types exist, false otherwise
     */
    private boolean hasClassificationTypes() throws AtlasBaseException {
        try {
            // Query for any vertex that is a type system vertex with TRAIT category
            AtlasGraphQuery query = graph.query().has(VERTEX_TYPE_PROPERTY_KEY, VERTEX_TYPE)
                    .has(TYPE_CATEGORY_PROPERTY_KEY, DataTypes.TypeCategory.TRAIT);

            Iterator<AtlasVertex> results = query.vertices().iterator();
            boolean hasClassifications = results.hasNext();
            
            LOG.info("Found classification types: {}", hasClassifications);
            return hasClassifications;
        } catch (Exception e) {
            LOG.error("Error checking for classification types", e);
            throw new AtlasBaseException("Error checking for classification types", e);
        }
    }
}