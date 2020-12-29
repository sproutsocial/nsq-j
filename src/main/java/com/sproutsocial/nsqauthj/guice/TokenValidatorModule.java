package com.sproutsocial.nsqauthj.guice;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.sproutsocial.nsqauthj.NsqAuthJConfiguration;
import com.sproutsocial.nsqauthj.validators.VaultTokenValidator;

public class TokenValidatorModule extends AbstractModule {

    private final NsqAuthJConfiguration config;

    public TokenValidatorModule(final NsqAuthJConfiguration config) {
        this.config = config;
    }

    @Provides
    @Singleton
    public VaultTokenValidator provideTokenValidator(MetricRegistry metricRegistry) throws Exception {
        return config.getTokenValidationFactory().build(
                config.getVaultClientFactory().build(),
                metricRegistry
        );
    }
}
