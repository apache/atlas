package org.apache.atlas.repository.store.graph.v2.preprocessor;

import com.google.common.base.Predicate;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

import static org.apache.atlas.repository.Constants.ATTRIBUTE_LINK;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
public class LinkPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(LinkPreProcessor.class);
    private static final Pattern REGEX_ONSITE_URL = Pattern.compile("(?:[\\p{L}\\p{N}\\\\\\.\\#@\\$%\\+&;\\-_~,\\?=/!]+|\\#(\\w)+)");
    private static final Pattern REGEX_OFFSITE_URL = Pattern.compile("\\s*(?:(?:ht|f)tps?://|mailto:)[\\p{L}\\p{N}]"
            + "[\\p{L}\\p{N}\\p{Zs}\\.\\#@\\$%\\+&;:\\-_~,\\?=/!\\(\\)]*+\\s*");
    private static final Predicate<String> REGEX_ON_OFFSITE_URL = matchesEither(REGEX_ONSITE_URL, REGEX_OFFSITE_URL);

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context, EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("LinkPreProcessor.processAttributes: pre processing {}, {}", entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;
        String link = (String) entity.getAttribute(ATTRIBUTE_LINK);

        switch (operation) {
            case CREATE:
            case UPDATE:
                processLink(link, operation.name());
                break;
        }

    }

    /**
     * Processes the link based on the provided operation.
     *
     * @param link     The link to be processed.
     * @param operation The operation to be performed.
     * @throws AtlasBaseException If the link is not valid or empty.
     */
    private void processLink(String link, String operation) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord(operation);

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
