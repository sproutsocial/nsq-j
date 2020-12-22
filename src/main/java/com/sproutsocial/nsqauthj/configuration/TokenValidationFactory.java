package com.sproutsocial.nsqauthj.configuration;

import com.bettercloud.vault.Vault;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.sproutsocial.nsqauthj.validators.VaultTokenValidator;

import javax.validation.constraints.NotNull;

public class TokenValidationFactory {

    @NotNull
    private String userTokenPath;

    @NotNull
    private String serviceTokenPath;

    @NotNull
    private int tokenTTL;

    @JsonProperty
    public String getServiceTokenPath() {
        return serviceTokenPath;
    }

    @JsonProperty
    public void setServiceTokenPath(String serviceTokenPath) {
        this.serviceTokenPath = serviceTokenPath;
    }

    @JsonProperty
    public String getUserTokenPath() {
        return userTokenPath;
    }

    @JsonProperty
    public void setUserTokenPath(String userTokenPath) {
        this.userTokenPath = userTokenPath;
    }

    @JsonProperty
    public int getTokenTTL() {
        return tokenTTL;
    }

    @JsonProperty
    public void setTokenTTL(int tokenTTL) {
        this.tokenTTL = tokenTTL;
    }

    public VaultTokenValidator build(Vault vault, MetricRegistry metricRegistry) {
        return new VaultTokenValidator(vault, userTokenPath, serviceTokenPath, tokenTTL, metricRegistry);
    }

}
