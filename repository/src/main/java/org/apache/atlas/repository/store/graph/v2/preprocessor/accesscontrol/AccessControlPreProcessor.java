package org.apache.atlas.repository.store.graph.v2.preprocessor.accesscontrol;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_CHANNEL_LINK;

public class AccessControlPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(AccessControlPreProcessor.class);

    // Regex reference - https://atlanhq.atlassian.net/browse/DG-1907?focusedCommentId=270252
    private static final Pattern REGEX_SLACK_CHANNEL_LINK = Pattern.compile("^https?://(?:[\\w-]+\\.)*slack\\.com(?:/.*)?$");
    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context, EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("AccessControlPreProcessor.processAttributes: pre processing {}, {}", entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;

        switch (operation) {
            case CREATE:
                processCreateAccessControlAsset(entity);
                break;
            case UPDATE:
                processUpdateAccessControlAsset(context, entity);
                break;
        }
    }

    private void processCreateAccessControlAsset(AtlasEntity entity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreateAccessControlAsset");

        validateChannelLink(entity);

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void processUpdateAccessControlAsset(EntityMutationContext context, AtlasEntity entity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateAccessControlAsset");

        validateChannelLink(entity);

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void validateChannelLink(AtlasEntity entity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("validateChannelLink");

        if (entity.hasAttribute(ATTR_CHANNEL_LINK)) {
            String channelLink = (String) entity.getAttribute(ATTR_CHANNEL_LINK);

            if (StringUtils.isNotEmpty(channelLink)) {
                Matcher channelLinkMatcher = REGEX_SLACK_CHANNEL_LINK.matcher(channelLink);

                if (!channelLinkMatcher.matches()) {
                    throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Please provide a valid URL for " + ATTR_CHANNEL_LINK);
                }
            }
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }
}
