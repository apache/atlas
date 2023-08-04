package org.apache.atlas.repository.store.graph.v2.preprocessor.resource;

import com.google.common.base.Predicate;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.CREATE;
import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.UPDATE;
import static org.apache.atlas.repository.Constants.ASSET_LINK_EDGE_LABEL;
import static org.apache.atlas.repository.Constants.ATTRIBUTE_LINK;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;

public class LinkPreProcessor extends AbstractResourcePreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(LinkPreProcessor.class);

    private static final Pattern REGEX_ONSITE_URL = Pattern.compile("(?:[\\p{L}\\p{N}\\\\\\.\\#@\\$%\\+&;\\-_~,\\?=/!]+|\\#(\\w)+)");
    private static final Pattern REGEX_OFFSITE_URL = Pattern.compile("\\s*(?:(?:ht|f)tps?://|mailto:)[\\p{L}\\p{N}]"
            + "[\\p{L}\\p{N}\\p{Zs}\\.\\#@\\$%\\+&;:\\-_~,\\?=/!\\(\\)]*+\\s*");
    private static final Predicate<String> REGEX_ON_OFFSITE_URL = matchesEither(REGEX_ONSITE_URL, REGEX_OFFSITE_URL);

    public LinkPreProcessor(AtlasTypeRegistry typeRegistry,
                            EntityGraphRetriever entityRetriever) {
        super(typeRegistry, entityRetriever);
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context, EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("LinkPreProcessor.processAttributes: pre processing {}, {}", entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;

        switch (operation) {
            case CREATE:
                processLinkCreate(entity);
                break;
            case UPDATE:
                processLinkUpdate(entity, context.getVertex(entity.getGuid()));
                break;
        }

    }

    private void processLinkCreate(AtlasEntity linkEntity) throws AtlasBaseException {
        validateLinkAttribute(linkEntity, CREATE.name());
    }

    private void processLinkUpdate(AtlasEntity linkEntity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processLinkUpdate");

        try {
            validateLinkAttribute(linkEntity, UPDATE.name());

            authorizeUpdate(linkEntity, vertex, ASSET_LINK_EDGE_LABEL);
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    /**
     * Validate the link based on the provided operation.
     *
     * @param linkEntity     The linkEntity to be processed.
     * @param operation The operation to be performed.
     * @throws AtlasBaseException If the link is not valid or empty.
     */
    private void validateLinkAttribute(AtlasEntity linkEntity, String operation) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord(operation);

        String link = (String) linkEntity.getAttribute(ATTRIBUTE_LINK);

        if (link == null || link.isEmpty()) {
            throw new AtlasBaseException("Link is empty for operation: " + operation);
        }
        if (!REGEX_ON_OFFSITE_URL.apply(link)) {
            throw new AtlasBaseException("Please provide a valid URL for operation: " + operation);
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private static Predicate<String> matchesEither(final Pattern a, final Pattern b) {
        return input -> a.matcher(input).matches() || b.matcher(input).matches();
    }
}
