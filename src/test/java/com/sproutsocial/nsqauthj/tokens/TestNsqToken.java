package com.sproutsocial.nsqauthj.tokens;

import com.bettercloud.vault.response.LogicalResponse;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestNsqToken {

    @Test
    public void fromVaultResponseNoUsername() {
        LogicalResponse response = mock(LogicalResponse.class);
        Map<String, String> responseData = new HashMap<>();
        responseData.put("topics", ".*");
        when(response.getData()).thenReturn(responseData);

        Optional<NsqToken> optionalNsqToken = NsqToken.fromVaultResponse(
                response,
                "asdf",
                NsqToken.TYPE.USER,
                300,
                "123.123.123.123"
        );

        assertFalse(optionalNsqToken.isPresent());
    }

    @Test
    public void fromVaultNoTopics() {
        LogicalResponse response = mock(LogicalResponse.class);
        Map<String, String> responseData = new HashMap<>();
        responseData.put("username", "some.developer");
        when(response.getData()).thenReturn(responseData);

        Optional<NsqToken> optionalNsqToken = NsqToken.fromVaultResponse(
                response,
                "asdf",
                NsqToken.TYPE.USER,
                300,
                "123.123.123.123"
        );

        assertFalse(optionalNsqToken.isPresent());

    }

    @Test
    public void fromVaultValidData() {
        LogicalResponse response = mock(LogicalResponse.class);
        Map<String, String> responseData = new HashMap<>();
        responseData.put("username", "some.developer");
        responseData.put("topics", "tw_engagement,fb_post");
        when(response.getData()).thenReturn(responseData);

        Optional<NsqToken> optionalNsqToken = NsqToken.fromVaultResponse(
                response,
                "asdf",
                NsqToken.TYPE.USER,
                300,
                "123.123.123.123"
        );

        assertTrue(optionalNsqToken.isPresent());
        NsqToken nsqToken = optionalNsqToken.get();
        assertEquals(nsqToken.getTopics(), Arrays.asList("tw_engagement", "fb_post"));
        assertEquals(nsqToken.getUsername(), "some.developer");
        assertEquals(nsqToken.getType(), NsqToken.TYPE.USER);
        assertEquals(nsqToken.getTtl(), 300);

    }

    @Test
    public void generatePublishOnlyToken() {
        Optional<NsqToken> optionalNsqToken = NsqToken.generatePublishOnlyToken(500, "123.123.123.123");
        assertTrue(optionalNsqToken.isPresent());
        NsqToken token = optionalNsqToken.get();
        assertEquals(token.getTtl(), 500);
        assertEquals(token.getUsername(), "123.123.123.123");
        assertEquals(token.getType(), NsqToken.TYPE.PUBLISH_ONLY);
        assertEquals(token.getTopics(), Arrays.asList(".*"));

    }
}