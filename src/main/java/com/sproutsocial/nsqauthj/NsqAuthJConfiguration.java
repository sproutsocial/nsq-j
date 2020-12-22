package com.sproutsocial.nsqauthj;

import com.bettercloud.vault.Vault;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.sproutsocial.configuration.metrics.MetricsFactory;
import com.sproutsocial.nsqauthj.configuration.TokenValidationFactory;
import com.sproutsocial.nsqauthj.configuration.VaultClientFactory;
import io.dropwizard.Configuration;

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
    private MetricsFactory metrics;

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

    @JsonProperty("metrics")
    public MetricsFactory getMetrics() {
        return metrics;
    }

    @JsonProperty("metrics")
    public void setMetrics(MetricsFactory metrics) {
        this.metrics = metrics;
    }}
