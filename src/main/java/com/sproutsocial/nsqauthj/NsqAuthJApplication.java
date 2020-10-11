package com.sproutsocial.nsqauthj;

import com.google.inject.Injector;
import com.hubspot.dropwizard.guicier.GuiceBundle;
import com.sproutsocial.configuration.dropwizard.DropwizardConfigCommonsFactoryFactory;
import com.sproutsocial.nsqauthj.guice.TokenValidatorModule;
import com.sproutsocial.nsqauthj.resources.AuthResource;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;


public class NsqAuthJApplication extends Application<NsqAuthJConfiguration> {

    private GuiceBundle<NsqAuthJConfiguration> guiceBundle;

    public static void main(String[] args) throws Exception {
        new NsqAuthJApplication().run(args);
    }

    @Override
    public String getName() {
        return "NsqAuthJ";
    }

    @Override
    public void initialize(Bootstrap<NsqAuthJConfiguration> bootstrap) {
        bootstrap.setConfigurationFactoryFactory(new DropwizardConfigCommonsFactoryFactory());

        guiceBundle =
                GuiceBundle.defaultBuilder(NsqAuthJConfiguration.class)
                        .enableGuiceEnforcer(false)
                        .modules(
                                new TokenValidatorModule()
                        )
                        .build();

        bootstrap.addBundle(guiceBundle);
    }

    @Override
    public void run(NsqAuthJConfiguration config, Environment env) {
        Injector injector = guiceBundle.getInjector();

        env.jersey().register(injector.getInstance(AuthResource.class));
    }
}