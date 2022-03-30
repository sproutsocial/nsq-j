package com.sproutsocial.nsqauthj.logging;

import ch.qos.logback.access.spi.IAccessEvent;
import com.google.common.collect.ImmutableMap;
import io.dropwizard.logging.json.AccessAttribute;
import io.dropwizard.logging.json.layout.AccessJsonLayout;
import io.dropwizard.logging.json.layout.JsonFormatter;
import io.dropwizard.logging.json.layout.TimestampFormatter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This layout will use the provided patterns to replace any matched groups with '<REDACTED>'.
 * It will not attempt to censor non-Strings.
 * The whole match is redacted, no effort is made to be cute with capturing groups.
 */
public class AccessSecretMaskingJsonLayout extends AccessJsonLayout {

    private List<Pattern> patterns;

    public AccessSecretMaskingJsonLayout(
            JsonFormatter jsonFormatter,
            TimestampFormatter timestampFormatter,
            Set<AccessAttribute> includes,
            Map<String, String> customFieldNames,
            Map<String, Object> additionalFields,
            List<Pattern> patterns
    ) {
        super(jsonFormatter, timestampFormatter, includes, customFieldNames, additionalFields);
        this.patterns = patterns;
    }

    @Override
    protected Map<String, Object> toJsonMap(IAccessEvent event) {
        Map<String, Object> jsonMap = super.toJsonMap(event);

        final ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        for (Map.Entry<String, Object> entry : jsonMap.entrySet()) {
            if (entry.getValue() instanceof String) {
                builder.put(
                        entry.getKey(),
                        redact((String) entry.getValue())
                );
            } else {
                builder.put(entry);
            }
        }
        return builder.build();
    }

    private String redact(String value) {
        String current = value;
        for (Pattern pattern : patterns) {
            final StringBuilder builder = new StringBuilder();
            int lastIndex = 0;
            Matcher matcher = pattern.matcher(value);
            while (matcher.find()) {
                builder.append(current, lastIndex, matcher.start()).append("<REDACTED>");
                lastIndex = matcher.end();
            }
            if (lastIndex < current.length()) {
                builder.append(current, lastIndex, current.length());
            }
            current = builder.toString();
        }
        return current;
    }
}
