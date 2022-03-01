package com.sproutsocial.nsqauthj.permissions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.sproutsocial.nsqauthj.tokens.NsqToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/*
This permission set is evaluated by NSQ when determining permissions for a connection.
As such, the attribute names here cannot be changed.
 */
public class NsqPermissionSet {
    private static final Logger auditLogger = LoggerFactory.getLogger("NsqPermissionAudit");


    public List<Authorization> getAuthorizations() {
        return authorizations;
    }

    public String getIdentityUrl() {
        return identityUrl;
    }

    public String getIdentity() {
        return identity;
    }

    public int getTtl() {
        return ttl;
    }

    public static class Authorization {
        static String subscribePermission = "subscribe";
        static String publishPermission = "publish";
        static String wildcardChannel = ".*";
        static String ephermeralChannel = ".*ephemeral";
        static ArrayList<String> allPermissions = new ArrayList<>(Arrays.asList(subscribePermission, publishPermission));

        @JsonProperty
        private String topic;

        @JsonProperty
        private List<String> channels;

        @JsonProperty
        private List<String> permissions;

        @JsonCreator
        public Authorization(@JsonProperty("topics") String topic, @JsonProperty("channels") List<String> channels, @JsonProperty("permissions") List<String> permissions) {
            this.topic = topic;
            this.channels = channels;
            this.permissions = permissions;
        }

        public String getTopic() {
            return topic;
        }

        public List<String> getChannels() {
            return channels;
        }

        public List<String> getPermissions() {
            return permissions;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final Authorization a = (Authorization) o;
            return (
                    Objects.equals(topic, a.topic) &&
                    Objects.equals(channels, a.channels) &&
                    Objects.equals(permissions, a.permissions)
            );
        }
    }

    @JsonProperty
    private List<Authorization> authorizations;

    @JsonProperty
    private String identityUrl;

    @JsonProperty
    private String identity;

    @JsonProperty
    private int ttl;

    @JsonCreator
    public NsqPermissionSet(@JsonProperty("authorization") List<Authorization> authorizations, @JsonProperty("identityUrl") String identityUrl, @JsonProperty("identity") String identity, @JsonProperty("ttl") int ttl) {
        this.authorizations = authorizations;
        this.identityUrl = identityUrl;
        this.identity = identity;
        this.ttl = ttl;
    }

    public static NsqPermissionSet fromNsqToken(NsqToken token, Boolean failOpen) {
        List<Authorization> authorizations = new ArrayList<>();

        for (String topic : token.getTopics()) {
            switch(token.getType()) {
                case SERVICE:
                    authorizations.add(new Authorization(
                            topic,
                            Arrays.asList(Authorization.wildcardChannel),
                            Authorization.allPermissions
                    ));
                    break;
                case USER:
                    authorizations.add(new Authorization(
                            topic,
                            Arrays.asList(Authorization.wildcardChannel),
                            Arrays.asList(Authorization.publishPermission)
                    ));
                    authorizations.add(new Authorization(
                            topic,
                            Arrays.asList(Authorization.ephermeralChannel),
                            Arrays.asList(Authorization.subscribePermission)
                    ));
                    break;
                default:
                    // If nsqauthj is failing open, leave the subscribe permission for all tokens.
                    List<String> permissions = Authorization.allPermissions;
                    if(!failOpen) {
                        permissions = Arrays.asList(Authorization.publishPermission);
                    }
                    authorizations.add(new Authorization(
                            topic,
                            Arrays.asList(Authorization.wildcardChannel),
                            permissions
                    ));
            }

        }

        NsqPermissionSet nsqPermissionSet = new NsqPermissionSet(
                authorizations,
                "",
                token.getUsername(),
                token.getTtl()
        );
        ObjectWriter writer = new ObjectMapper().writer();
        try {
            auditLogger.info("NSQ Token: " + writer.writeValueAsString(token) + " Permission Set: " + writer.writeValueAsString(nsqPermissionSet));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return nsqPermissionSet;
    }
}
