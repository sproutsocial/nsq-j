package com.sproutsocial.nsqauthj.permissions;

import com.sproutsocial.nsqauthj.tokens.NsqToken;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestNsqPermissionSet {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {
                        Arrays.asList("fb_whatever", "tw_whatever"),
                        "george.costanza",
                        NsqToken.TYPE.USER,
                        300,
                        "127.0.0.1",
                        Arrays.asList(".*ephemeral"),
                        Arrays.asList("subscribe", "publish")
                },
                {
                        Arrays.asList("fb_whatever"),
                        "service.service",
                        NsqToken.TYPE.SERVICE,
                        300,
                        "127.0.0.1",
                        Arrays.asList(".*"),
                        Arrays.asList("subscribe", "publish")
                },
                {
                        Arrays.asList("fb_whatever"),
                        "127.0.0.1",
                        NsqToken.TYPE.PUBLISH_ONLY,
                        300,
                        "127.0.0.1",
                        Arrays.asList(".*"),
                        Arrays.asList("publish")
                }
        });
    }

    @Parameterized.Parameter
    public List<String> topics;

    @Parameterized.Parameter(1)
    public String username;

    @Parameterized.Parameter(2)
    public NsqToken.TYPE nsqType;

    @Parameterized.Parameter(3)
    public int ttl;

    @Parameterized.Parameter(4)
    public String ip;

    @Parameterized.Parameter(5)
    public List<String> channels;

    @Parameterized.Parameter(6)
    public List<String> permissions;

    @Test
    public void test() {
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