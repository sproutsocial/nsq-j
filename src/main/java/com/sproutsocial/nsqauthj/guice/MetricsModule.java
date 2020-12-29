package com.sproutsocial.nsqauthj.guice;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.sproutsocial.configuration.metrics.MetricsFactory;
import com.sproutsocial.nsqauthj.NsqAuthJConfiguration;

public class MetricsModule extends AbstractModule {
    private final NsqAuthJConfiguration configuration;

    public MetricsModule(NsqAuthJConfiguration configuration) {
        this.configuration = configuration;
    }

    @Provides
    @Singleton
    public MetricRegistry getMetricRegistry() throws Exception {
        MetricsFactory metrics = configuration.getMetrics();
        return metrics.build();
    }
}
