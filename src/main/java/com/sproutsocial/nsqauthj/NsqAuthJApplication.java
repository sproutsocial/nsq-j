package com.sproutsocial.nsqauthj;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.sproutsocial.configuration.dropwizard.DropwizardConfigCommonsFactoryFactory;
import com.sproutsocial.nsqauthj.guice.MetricsModule;
import com.sproutsocial.nsqauthj.guice.TokenValidatorModule;
import com.sproutsocial.nsqauthj.resources.AuthResource;
import com.sproutsocial.platform.Heartbeater;
import com.sproutsocial.platform.HeartbeaterImpl;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;


public class NsqAuthJApplication extends Application<NsqAuthJConfiguration> {

    public static void main(String[] args) throws Exception {
        new NsqAuthJApplication().run(args);
    }

    @Override
    public String getName() {
        return "NsqAuthJ";
    }

    public void initialize(Bootstrap<NsqAuthJConfiguration> bootstrap) {
        bootstrap.setConfigurationFactoryFactory(new DropwizardConfigCommonsFactoryFactory());
    }

    @Override
    public void run(NsqAuthJConfiguration config, Environment env) {
        Injector injector = Guice.createInjector(
                new TokenValidatorModule(config),
                new MetricsModule(config),
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(Heartbeater.class).to(HeartbeaterImpl.class).in(Singleton.class);
                    }
                }
        );

        env.jersey().register(injector.getInstance(AuthResource.class));
    }
}