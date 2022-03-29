package com.sproutsocial.nsqauthj.guice;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.sproutsocial.configuration.metrics.MetricsFactory;
import com.sproutsocial.nsqauthj.NsqAuthJConfiguration;
import com.sproutsocial.nsqauthj.validators.VaultTokenValidator;
import com.sproutsocial.platform.Heartbeater;

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

    @Provides
    @Singleton
    public MetricRegistry getMetricRegistry() throws Exception {
        MetricsFactory metrics = config.getMetrics();
        return metrics.build();
    }

    @Provides
    @Singleton
    public Heartbeater getHeartbeater() throws Exception {
        return config.getHeartbeaterFactory().build();
    }
}
