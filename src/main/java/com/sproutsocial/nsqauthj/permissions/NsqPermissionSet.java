package com.sproutsocial.nsqauthj.permissions;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.sproutsocial.nsqauthj.tokens.NsqToken;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/*
This permission set is evaluated by NSQ when determining permissions for a connection.
As such, the attribute names here cannot be changed.
 */
public class NsqPermissionSet {
    public static class Authorization {
        @JsonProperty
        private String topic;

        @JsonProperty
        private List<String> channels;

        @JsonProperty
        private List<String> permissions;

        public Authorization(String topic, List<String> channels, List<String> permissions) {
            this.topic = topic;
            this.channels = channels;
            this.permissions = permissions;
        }

    }

    @JsonProperty
    List<Authorization> authorizations;

    @JsonProperty
    private String identityUrl;

    @JsonProperty
    private String identity;

    @JsonProperty
    private int ttl;

    public NsqPermissionSet(List<Authorization> authorizations, String identityUrl, String identity, int ttl) {
        this.authorizations = authorizations;
        this.identityUrl = identityUrl;
        this.identity = identity;
        this.ttl = ttl;
    }

    public static NsqPermissionSet fromNsqToken(NsqToken token) {
        List<String> channels = new ArrayList<>();
        ArrayList<String> permissions = new ArrayList<>(Arrays.asList("subscribe", "publish"));
        switch(token.getType()) {
            case SERVICE:
                channels.add(".*");
                break;
            case USER:
                channels.add(".*ephemeral");
                break;
            default:
                channels.add(".*");
                permissions.remove("subscribe");

        }

        List<Authorization> authorizations = new ArrayList<>();

        for (String topic : token.getTopics()) {
            authorizations.add(new Authorization(
                    topic,
                    channels,
                    permissions
            ));
        }

        return new NsqPermissionSet(
                authorizations,
                "",
                token.getUsernname(),
                token.getTtl()
        );
    }
}
