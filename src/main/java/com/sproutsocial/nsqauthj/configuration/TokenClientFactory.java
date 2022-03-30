package com.sproutsocial.nsqauthj.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.constraints.NotNull;

public class TokenClientFactory extends AbstractVaultClientFactory {
    @NotNull
    private String token;

    @JsonProperty
    String getToken() {
        return token;
    }

    @JsonProperty
    public void setToken(String token) {
        this.token = token;
    }
}
