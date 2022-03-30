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
    private MetricRegistry metricRegistry;

    public TokenValidatorModule(final NsqAuthJConfiguration config, MetricRegistry metricRegistry) {
        this.config = config;
        this.metricRegistry = metricRegistry;
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
        return metricRegistry;
    }

    @Provides
    @Singleton
    public Heartbeater getHeartbeater() throws Exception {
        return config.getHeartbeaterFactory().build();
    }
}
