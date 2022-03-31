package com.sproutsocial.nsqauthj.logging;


import ch.qos.logback.access.spi.IAccessEvent;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.LayoutBase;
import com.google.common.collect.ImmutableList;
import com.sproutsocial.nsqauthj.configuration.AccessSecretMaskingJsonLayoutFactory;
import java.time.ZoneId;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Pattern;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AccessSecretMaskingJsonLayoutTest {

    private LayoutBase<IAccessEvent> layoutBase;

    @BeforeEach
    public void setUp() {
        LoggerContext context = new LoggerContext();
        AccessSecretMaskingJsonLayoutFactory factory = new AccessSecretMaskingJsonLayoutFactory();
        factory.setPatterns(ImmutableList.of(Pattern.compile("secret=[^&]*")));
        layoutBase = factory.build(context, TimeZone.getTimeZone(ZoneId.systemDefault()));
    }

    @Test
    public void factoryMakesAAccessSecretMaskingJsonLayout() {
        assertTrue(layoutBase instanceof AccessSecretMaskingJsonLayout);
    }

    @Test
    public void secretMasked() {
        AccessSecretMaskingJsonLayout layout = (AccessSecretMaskingJsonLayout) layoutBase;
        IAccessEvent event = mock(IAccessEvent.class);
        when(event.getRequestURI()).thenReturn("/auth?remote=blah&secret=shhhh&tls=false");
        Map<String, Object> jsonMap = layout.toJsonMap(event);
        assertEquals("/auth?remote=blah&<REDACTED>&tls=false", jsonMap.get("uri"));
    }

    @Test
    public void multipleSecretsMasked() {
        AccessSecretMaskingJsonLayout layout = (AccessSecretMaskingJsonLayout) layoutBase;
        IAccessEvent event = mock(IAccessEvent.class);
        when(event.getRequestURI()).thenReturn("/auth?remote=blah&secret=shhhh&tls=false&secret=hush");
        Map<String, Object> jsonMap = layout.toJsonMap(event);
        assertEquals("/auth?remote=blah&<REDACTED>&tls=false&<REDACTED>", jsonMap.get("uri"));
    }

    @Test
    public void nonSecretLeftAlone() {
        AccessSecretMaskingJsonLayout layout = (AccessSecretMaskingJsonLayout) layoutBase;
        IAccessEvent event = mock(IAccessEvent.class);
        when(event.getRequestURI()).thenReturn("/auth?remote=blah&quiet=shhhh&tls=false");
        Map<String, Object> jsonMap = layout.toJsonMap(event);
        assertEquals("/auth?remote=blah&quiet=shhhh&tls=false", jsonMap.get("uri"));
    }
}
