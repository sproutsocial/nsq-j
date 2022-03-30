package com.sproutsocial.nsqauthj.configuration;

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;

public abstract class AbstractVaultClientFactory implements VaultClientFactory {

    @NotNull
    private String addr;

    private int engineVersion = 2;


    @NotNull
    private int openTimeout = 5;

    @NotNull
    private int readTimeout = 5;

    @NotNull
    private boolean renewable = false;

    @NotNull
    private int renewInterval = 3600; // renew once an hour

    abstract String getToken() throws Exception;

    /**
     * Creates a Vault client with the provided token and returns the Vault object
     */
    public Vault build() throws Exception {
        String token = getToken();
        VaultConfig config = new VaultConfig()
                .address(addr)
                .token(token)
                .openTimeout(openTimeout)
                .readTimeout(readTimeout)
                .build();
        Vault vault = new Vault(config, engineVersion);
        if (renewable) {
            startRenewingToken(token, vault);
        }
        return vault;
    }

    protected void startRenewingToken(String token, Vault vault) {
        ScheduledExecutorService executorService = Executors
                .newSingleThreadScheduledExecutor();
        VaultTokenRenewer renewer = new VaultTokenRenewer(token, vault, getRenewInterval());
        executorService.scheduleAtFixedRate(renewer::renewToken, renewInterval, renewInterval, TimeUnit.SECONDS);

    }

    @JsonProperty
    public String getAddr() {
        return addr;
    }

    @JsonProperty
    public void setAddr(String addr) {
        this.addr = addr;
    }

    @JsonProperty
    public int getEngineVersion() {
        return engineVersion;
    }

    @JsonProperty
    public void setEngineVersion(int engineVersion) {
        this.engineVersion = engineVersion;
    }

    @JsonProperty
    public int getOpenTimeout() {
        return openTimeout;
    }

    @JsonProperty
    public void setOpenTimeout(int openTimeout) {
        this.openTimeout = openTimeout;
    }

    @JsonProperty
    public int getReadTimeout() {
        return readTimeout;
    }

    @JsonProperty
    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }

    public boolean isRenewable() {
        return renewable;
    }

    @JsonProperty
    public void setRenewable(boolean renewable) {
        this.renewable = renewable;
    }

    public int getRenewInterval() {
        return renewInterval;
    }

    @JsonProperty
    public void setRenewInterval(int renewInterval) {
        this.renewInterval = renewInterval;
    }
}
