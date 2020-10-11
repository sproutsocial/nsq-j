package com.sproutsocial.nsqauthj.guice;


import com.bettercloud.vault.Vault;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.hubspot.dropwizard.guicier.DropwizardAwareModule;
import com.sproutsocial.nsqauthj.NsqAuthJConfiguration;

public class VaultModule extends DropwizardAwareModule<NsqAuthJConfiguration> {

    public VaultModule() { }

    public void configure(Binder binder) { }

    @Provides
    @Singleton
    public Vault provideVault() throws Exception {
        return getConfiguration().getVaultClientFactory().build();
    }

}
