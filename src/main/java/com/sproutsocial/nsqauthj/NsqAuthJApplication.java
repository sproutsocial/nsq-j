package com.sproutsocial.nsqauthj;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.sproutsocial.configuration.dropwizard.DropwizardConfigCommonsFactoryFactory;
import com.sproutsocial.nsqauthj.guice.TokenValidatorModule;
import com.sproutsocial.nsqauthj.resources.AuthResource;
import com.sproutsocial.platform.dropwizard.commons.healthcheck.HealthCheckResource;
import io.dropwizard.Application;
import io.dropwizard.health.conf.HealthConfiguration;
import io.dropwizard.health.core.HealthCheckBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;


public class NsqAuthJApplication extends Application<NsqAuthJConfiguration> {

    public static void main(String[] args) throws Exception {
        new NsqAuthJApplication().run(args);
    }

    @Override
    public String getName() { return "nsqauthj"; }

    public void initialize(Bootstrap<NsqAuthJConfiguration> bootstrap) {
        bootstrap.setConfigurationFactoryFactory(new DropwizardConfigCommonsFactoryFactory<>());
        bootstrap.addBundle(new HealthCheckBundle<NsqAuthJConfiguration>() {
            @Override
            protected HealthConfiguration getHealthConfiguration(NsqAuthJConfiguration nsqAuthJConfiguration) {
                return nsqAuthJConfiguration.getHealthConfiguration();
            }
        });
    }

    @Override
    public void run(NsqAuthJConfiguration config, Environment env) {
        Injector injector = Guice.createInjector(new TokenValidatorModule(config, env.metrics()));
        env.jersey().register(injector.getInstance(AuthResource.class));
        env.jersey().register(new HealthCheckResource("nsqauthj"));
    }
}