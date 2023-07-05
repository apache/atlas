package org.apache.atlas.service.metrics;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class MetricRegistryCondition implements Condition {
    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        try {
            String metricImpl = ApplicationProperties.get().getString("atlas.prometheus.service.impl");
            return Objects.nonNull(metricImpl) && metricImpl.equals(MetricsRegistryServiceImpl.class.getCanonicalName());
        } catch (AtlasException e) {
            return false;
        }
    }
}
