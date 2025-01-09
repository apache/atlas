package org.apache.atlas.web.filters;import ch.qos.logback.classic.spi.ILoggingEvent;


import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;
import org.slf4j.MDC;

public class MDCFilter extends Filter<ILoggingEvent> {

    private String mdcKey;
    private String mdcValue;

    public void setMdcKey(String mdcKey) {
        this.mdcKey = mdcKey;
    }

    public void setMdcValue(String mdcValue) {
        this.mdcValue = mdcValue;
    }

    @Override
    public FilterReply decide(ILoggingEvent event) {
        // Check if the MDC key-value matches
        String value = MDC.get(mdcKey);
        if (value != null && value.equals(mdcValue)) {
            return FilterReply.ACCEPT;
        }
        return FilterReply.DENY;
    }
}
