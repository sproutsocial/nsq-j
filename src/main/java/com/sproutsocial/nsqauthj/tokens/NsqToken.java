package com.sproutsocial.nsqauthj.tokens;

import com.bettercloud.vault.response.LogicalResponse;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/*
Represents a username and topic combination stored in Vault.
 */
public class NsqToken {
    @JsonProperty
    private final TYPE type;

    @JsonProperty
    private List<String> topics;

    @JsonProperty
    private String usernname;

    @JsonProperty
    private int ttl;

    @JsonProperty
    private String remoteAddr;

    public static enum TYPE {
        USER,
        SERVICE,
        PUBLISH_ONLY
    }

    public NsqToken(List<String> topics, String usernname, TYPE type, int ttl, String remoteAddr) {
        this.topics = topics;
        this.usernname = usernname;
        this.ttl = ttl;
        this.type = type;
        this.remoteAddr = remoteAddr;
    }

    public static Optional<NsqToken> fromVaultResponse(LogicalResponse response, TYPE type, int ttl, String remoteAddr) {
        Map<String, String> data = response.getData();
        if (!data.containsKey("username") || !data.containsKey("topics")) {
            // Log a warning here
            return Optional.empty();
        }

        List<String> topics = Stream.of(data.get("topics").split(",", -1)).collect(Collectors.toList());
        String username = data.get("username");
        return Optional.of(new NsqToken(topics, username, type, ttl, remoteAddr));
    }

    public static Optional<NsqToken> generatePublishOnlyToken(int ttl, String ipaddress) {
        // User the ipaddress as the username when generating a publish only token
        return Optional.of(new NsqToken(
                Arrays.asList(".*"),
                ipaddress,
                TYPE.PUBLISH_ONLY,
                ttl,
                ipaddress));
    }

    public List<String> getTopics() {
        return topics;
    }

    public String getUsernname() {
        return usernname;
    }

    public int getTtl() {
        return ttl;
    }

    public TYPE getType() {
        return type;
    }

}
