package org.apache.atlas;

import java.util.logging.LogRecord;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;

public class LoggingFilterCustom extends Filter {
    private static final Pattern PATTERN;

    static {
        PATTERN = Pattern.compile("^slave: redis:.* is down");
    }
//Pattern.compile("^slave: redis:.* is down").matcher("slave: redis://10.192.41.100:6379 is down").matches()
    @Override
    public int decide(LoggingEvent event) {
        String message = (String) event.getMessage();

        final Matcher m = PATTERN.matcher(message);
        return m.matches() ? DENY : NEUTRAL;
    }
}
