package com.sproutsocial.nsqauthj.guice;

import com.bettercloud.vault.Vault;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.hubspot.dropwizard.guicier.DropwizardAwareModule;
import com.sproutsocial.nsqauthj.NsqAuthJConfiguration;
import com.sproutsocial.nsqauthj.validators.VaultTokenValidator;

public class TokenValidatorModule extends DropwizardAwareModule<NsqAuthJConfiguration> {

    public TokenValidatorModule() { }

    public void configure(Binder binder) { }

    @Provides
    @Singleton
    public VaultTokenValidator provideTokenValidator() throws Exception {
        return getConfiguration().getTokenValidationFactory().build(
                getConfiguration().getVaultClientFactory().build()
        );
    }
}
