package com.sproutsocial.nsqauthj.configuration;

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.VaultException;
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractVaultClientFactory implements VaultClientFactory {

    private static Logger logger = LoggerFactory.getLogger(VaultClientFactory.class);

    private VaultTokenRenewer tokenRenewer = null;

    @NotNull
    private String addr;

    private int engineVersion = 2;

    @NotNull
    private int openTimeout = 5;

    @NotNull
    private int readTimeout = 5;

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
        startRenewingToken(vault);
        return vault;
    }

    protected void startRenewingToken(Vault vault) {
        try {
            tokenRenewer = new VaultTokenRenewer(vault);
            tokenRenewer.startRenewing();
        }
        catch (VaultException e) {
            logger.error("Unable to set up token renewal scheduler!", e);
        }

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

}
