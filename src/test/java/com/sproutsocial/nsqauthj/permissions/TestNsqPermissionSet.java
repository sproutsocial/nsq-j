package com.sproutsocial.nsqauthj.permissions;

import com.sproutsocial.nsqauthj.tokens.NsqToken;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class TestNsqPermissionSet {

    public static Stream<Arguments> data() {
        return Stream.of(
                Arguments.of(
                        Arrays.asList("fb_whatever", "tw_whatever"),
                        "george.costanza",
                        NsqToken.TYPE.USER,
                        300,
                        "127.0.0.1",
                        Arrays.asList(".*ephemeral"),
                        Arrays.asList("subscribe", "publish")
                ),
                Arguments.of(
                        Arrays.asList("fb_whatever"),
                        "service.service",
                        NsqToken.TYPE.SERVICE,
                        300,
                        "127.0.0.1",
                        Arrays.asList(".*"),
                        Arrays.asList("subscribe", "publish")
                ),
                Arguments.of(
                        Arrays.asList("fb_whatever"),
                        "127.0.0.1",
                        NsqToken.TYPE.PUBLISH_ONLY,
                        300,
                        "127.0.0.1",
                        Arrays.asList(".*"),
                        Arrays.asList("publish")
                )
        );
    }

    @ParameterizedTest
    @MethodSource("data")
    public void test(
            List<String> topics,
            String username,
            NsqToken.TYPE nsqType,
            int ttl,
            String ip,
            List<String> channels,
            List<String> permissions
    ) {
        NsqToken nsqToken = new NsqToken(
                topics,
                username,
                nsqType,
                ttl,
                ip
        );

        NsqPermissionSet permissionSet = NsqPermissionSet.fromNsqToken(nsqToken);

        for (NsqPermissionSet.Authorization authorization : permissionSet.getAuthorizations()) {
            assertEquals(authorization.getChannels(), channels);
            assertEquals(authorization.getPermissions(), permissions);
        }

        assertEquals(
                permissionSet.getAuthorizations().size(),
                topics.size()
        );
    }
}