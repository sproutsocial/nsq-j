package com.sproutsocial.nsqauthj.configuration;

import ch.qos.logback.access.spi.IAccessEvent;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.LayoutBase;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.sproutsocial.nsqauthj.logging.AccessSecretMaskingJsonLayout;
import io.dropwizard.logging.json.AccessJsonLayoutBaseFactory;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Pattern;
import javax.validation.constraints.NotNull;

@JsonTypeName("access-secret-masking-json")
public class AccessSecretMaskingJsonLayoutFactory extends AccessJsonLayoutBaseFactory {

    @NotNull
    private List<Pattern> patterns;

    @JsonProperty("patterns")
    public List<Pattern> getPatterns() {
        return patterns;
    }

    @JsonProperty("patterns")
    public void setPatterns(List<Pattern> patterns) {
        this.patterns = patterns;
    }

    public AccessSecretMaskingJsonLayoutFactory() {
        super();
    }

    @Override
    public LayoutBase<IAccessEvent> build(LoggerContext context, TimeZone timeZone) {
        final AccessSecretMaskingJsonLayout jsonLayout = new AccessSecretMaskingJsonLayout(
                createDropwizardJsonFormatter(),
                createTimestampFormatter(timeZone),
                getIncludes(),
                getCustomFieldNames(),
                getAdditionalFields(),
                patterns);
        jsonLayout.setContext(context);
        jsonLayout.setRequestHeaders(getRequestHeaders());
        jsonLayout.setResponseHeaders(getResponseHeaders());
        jsonLayout.setRequestAttributes(getRequestAttributes());
        return jsonLayout;
    }
}
