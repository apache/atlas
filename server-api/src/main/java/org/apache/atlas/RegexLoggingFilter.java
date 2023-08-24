package org.apache.atlas;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;

public class RegexLoggingFilter extends Filter {
    private Pattern pattern;

    public void setPatternString(String patternString) {
        pattern = Pattern.compile(patternString);
    }

    @Override
    public int decide(LoggingEvent event) {
        String message = (String) event.getMessage();

        if (event.getLevel().equals(Level.WARN)) {
            final Matcher m = pattern.matcher(message);
            return m.matches() ? DENY : NEUTRAL;
        }
        return NEUTRAL;
    }
}
