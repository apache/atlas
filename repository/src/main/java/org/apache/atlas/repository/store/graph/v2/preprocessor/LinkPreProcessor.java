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
    public static final Pattern REGEX_ONSITE_URL = Pattern.compile("(?:[\\p{L}\\p{N}\\\\\\.\\#@\\$%\\+&;\\-_~,\\?=/!]+|\\#(\\w)+)");
    public static final Pattern REGEX_OFFSITE_URL = Pattern.compile("\\s*(?:(?:ht|f)tps?://|mailto:)[\\p{L}\\p{N}]"
            + "[\\p{L}\\p{N}\\p{Zs}\\.\\#@\\$%\\+&;:\\-_~,\\?=/!\\(\\)]*+\\s*");
    public static final Predicate<String> REGEX_ON_OFFSITE_URL = matchesEither(REGEX_ONSITE_URL, REGEX_OFFSITE_URL);

    public LinkPreProcessor() {
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context, EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("LinkPreProcessor.processAttributes: pre processing {}, {}", entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;
        String link = (String) entity.getAttribute(ATTRIBUTE_LINK);

        switch (operation) {
            case CREATE:
                processCreateLink(link);
                break;
            case UPDATE:
                processUpdateLink(link);
                break;
        }
    }

    private void processCreateLink(String link) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreateLink");

        // If link is not any format of link, throw exception
        if (link == null || link.isEmpty()) {
            throw new AtlasBaseException("Link is empty");
        }
        // If link is not a valid link, throw exception
        if (!REGEX_ON_OFFSITE_URL.apply(link)) {
            throw new AtlasBaseException("Please provide a valid URL");
        }

        RequestContext.get().endMetricRecord(metricRecorder);

    }

    private void processUpdateLink(String link) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateLink");
        // If link is not any format of link, throw exception
        if (link == null || link.isEmpty()) {
            throw new AtlasBaseException("Link is empty");
        }
        // If link is not a valid link, throw exception
        if (!REGEX_ON_OFFSITE_URL.apply(link)) {
            throw new AtlasBaseException("Please provide a valid URL");
        }
        RequestContext.get().endMetricRecord(metricRecorder);

    }

    private static Predicate<String> matchesEither(final Pattern a, final Pattern b) {
        return new Predicate<String>() {
            public boolean apply(String s) {
                return a.matcher(s).matches() || b.matcher(s).matches();
            }
        };
    }


}
