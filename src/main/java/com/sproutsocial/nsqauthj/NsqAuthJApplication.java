package com.sproutsocial.nsqauthj;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.sproutsocial.configuration.dropwizard.DropwizardConfigCommonsFactoryFactory;
import com.sproutsocial.nsqauthj.guice.TokenValidatorModule;
import com.sproutsocial.nsqauthj.resources.AuthResource;
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
                new TokenValidatorModule(config)
        );

        env.jersey().register(injector.getInstance(AuthResource.class));
    }
}