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

    private int ttl;

    public static enum TYPE {
        USER,
        SERVICE,
        PUBLISH_ONLY
    }

    public NsqToken(List<String> topics, String usernname, TYPE type, int ttl) {
        this.topics = topics;
        this.usernname = usernname;
        this.ttl = ttl;
        this.type = type;
    }

    public static Optional<NsqToken> fromVaultResponse(LogicalResponse response, TYPE type, int ttl) {
        Map<String, String> data = response.getData();
        if (!data.containsKey("username") || !data.containsKey("topics")) {
            // Log a warning here
            return Optional.empty();
        }

        List<String> topics = Stream.of(data.get("topics").split(",", -1)).collect(Collectors.toList());
        String username = data.get("username");
        return Optional.of(new NsqToken(topics, username, type, ttl));
    }

    public static Optional<NsqToken> generatePublishOnlyToken(int ttl, String ipaddress) {
        return Optional.of(new NsqToken(
                Arrays.asList(".*"),
                ipaddress,
                TYPE.PUBLISH_ONLY,
                ttl
        ));
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
