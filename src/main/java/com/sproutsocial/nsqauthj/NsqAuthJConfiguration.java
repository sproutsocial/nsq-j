package com.sproutsocial.nsqauthj;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.sproutsocial.configuration.heartbeater.HeartbeaterFactory;
import com.sproutsocial.nsqauthj.configuration.TokenValidationFactory;
import com.sproutsocial.nsqauthj.configuration.VaultClientFactory;
import io.dropwizard.Configuration;

import io.dropwizard.health.conf.HealthConfiguration;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class NsqAuthJConfiguration extends Configuration {
    @Valid
    @NotNull
    private VaultClientFactory vaultClientFactory;

    @Valid
    @NotNull
    private TokenValidationFactory tokenValidationFactory;

    @Valid
    @NotNull
    private HeartbeaterFactory heartbeaterFactory = new HeartbeaterFactory.NoOpHeartbeaterFactory();

    @Valid
    @NotNull
    private HealthConfiguration healthConfiguration = new HealthConfiguration();


    @JsonProperty("vault")
    public VaultClientFactory getVaultClientFactory() {
        return vaultClientFactory;
    }

    @JsonProperty("vault")
    public void setVaultClientFactory(VaultClientFactory vaultClientFactory) {
        this.vaultClientFactory = vaultClientFactory;
    }

    @JsonProperty("tokenValidation")
    public TokenValidationFactory getTokenValidationFactory() {
        return tokenValidationFactory;
    }

    @JsonProperty("tokenValidation")
    public void setTokenValidationFactory(TokenValidationFactory tokenValidationFactory) {
        this.tokenValidationFactory = tokenValidationFactory;
    }

    @JsonProperty("heartbeater")
    public HeartbeaterFactory getHeartbeaterFactory() { return  heartbeaterFactory; }

    @JsonProperty("heartbeater")
    public void setHeartbeaterFactory(HeartbeaterFactory heartbeaterFactory) {
        this.heartbeaterFactory = heartbeaterFactory;
    }

    @JsonProperty("health")
    public HealthConfiguration getHealthConfiguration() {
        return healthConfiguration;
    }

    @JsonProperty("health")
    public void setHealthConfiguration(final HealthConfiguration healthConfiguration) {
        this.healthConfiguration = healthConfiguration;
    }

}