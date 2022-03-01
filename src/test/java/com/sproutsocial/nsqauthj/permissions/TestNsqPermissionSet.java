package com.sproutsocial.nsqauthj.permissions;

import com.sproutsocial.nsqauthj.tokens.NsqToken;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestNsqPermissionSet {

    public static Stream<Arguments> data() {
        return Stream.of(
                Arguments.of(
                        Arrays.asList("fb_whatever", "tw_whatever"),
                        "george.costanza",
                        NsqToken.TYPE.USER,
                        300,
                        "127.0.0.1",
                        // Users should have `publish` on any channel (wildcard -- .*) and
                        // `subscribe` on ephemeral channels (.*ephemeral)
                        Arrays.asList(
                                new NsqPermissionSet.Authorization(
                                        "fb_whatever",
                                        Arrays.asList(NsqPermissionSet.Authorization.wildcardChannel),
                                        Arrays.asList(NsqPermissionSet.Authorization.publishPermission)
                                ),
                                new NsqPermissionSet.Authorization(
                                        "fb_whatever",
                                        Arrays.asList(NsqPermissionSet.Authorization.ephermeralChannel),
                                        Arrays.asList(NsqPermissionSet.Authorization.subscribePermission)
                                ),
                                new NsqPermissionSet.Authorization(
                                        "tw_whatever",
                                        Arrays.asList(NsqPermissionSet.Authorization.wildcardChannel),
                                        Arrays.asList(NsqPermissionSet.Authorization.publishPermission)
                                ),
                                new NsqPermissionSet.Authorization(
                                        "tw_whatever",
                                        Arrays.asList(NsqPermissionSet.Authorization.ephermeralChannel),
                                        Arrays.asList(NsqPermissionSet.Authorization.subscribePermission)
                                )
                        ),
                        false
                ),
                Arguments.of(
                        Arrays.asList("fb_whatever"),
                        "service.service",
                        NsqToken.TYPE.SERVICE,
                        300,
                        "127.0.0.1",
                        // Services should have `subscribe` and `publish` on any channel (wildcard -- .*)
                        Arrays.asList(
                                new NsqPermissionSet.Authorization(
                                        "fb_whatever",
                                        Arrays.asList(NsqPermissionSet.Authorization.wildcardChannel),
                                        NsqPermissionSet.Authorization.allPermissions
                                )
                        ),
                        false
                ),
                Arguments.of(
                        Arrays.asList("fb_whatever"),
                        "127.0.0.1",
                        NsqToken.TYPE.PUBLISH_ONLY,
                        300,
                        "127.0.0.1",
                        // Publish only tokens should only have the `publish` permission on the wildcard channel
                        Arrays.asList(
                                new NsqPermissionSet.Authorization(
                                        "fb_whatever",
                                        Arrays.asList(NsqPermissionSet.Authorization.wildcardChannel),
                                        Arrays.asList(NsqPermissionSet.Authorization.publishPermission)
                                )
                        ),
                        false
                ),
                Arguments.of(
                        Arrays.asList("fb_whatever"),
                        "127.0.0.1",
                        NsqToken.TYPE.PUBLISH_ONLY,
                        300,
                        "127.0.0.1",
                        // Publish only tokens should only have the all permission on the wildcard channel if we are failing open
                        Arrays.asList(
                                new NsqPermissionSet.Authorization(
                                        "fb_whatever",
                                        Arrays.asList(NsqPermissionSet.Authorization.wildcardChannel),
                                        NsqPermissionSet.Authorization.allPermissions
                                )
                        ),
                        true
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
            List<NsqPermissionSet.Authorization> authorizations,
            Boolean failOpen
    ) {
        NsqToken nsqToken = new NsqToken(
                topics,
                username,
                nsqType,
                ttl,
                ip
        );

        NsqPermissionSet permissionSet = NsqPermissionSet.fromNsqToken(nsqToken, failOpen);

        assertTrue(permissionSet.getAuthorizations().equals(authorizations));
    }
}