package com.sproutsocial.nsqauthj.configuration;

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.response.AuthResponse;
import com.bettercloud.vault.response.LookupResponse;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "type"
)
@JsonSubTypes({
        @JsonSubTypes.Type(name = "token", value = VaultClientFactory.Token.class),
        @JsonSubTypes.Type(name = "approle", value = VaultClientFactory.Approle.class)
})
public interface VaultClientFactory {
    Vault build() throws Exception;

    /**
     * Automates periodic token renewal
     */
    class VaultTokenRenewer {

        private static Logger logger = LoggerFactory.getLogger(VaultTokenRenewer.class);

        private final String token;
        private final Vault vault;
        private final int ttl;


        /**
         * @param token : The Vault authentication token used to establish a Vault connection
         * @param vault : A Vault instance
         * @param increment : How much to increment the renewal by in seconds (for instance, setting this to 30 will add 30 seconds to the current expiration time)
         */
        public VaultTokenRenewer(String token, Vault vault, int increment) {
           this.token = token;
           this.vault = vault;
           this.ttl = increment;
        }

        /**
         * Makes the call to renew the token
         */
        public void renewToken() {
            try {
                AuthResponse authResponse = vault.auth().renewSelf(ttl);
                logger.info(authResponse.getTokenAccessor() + " renewed for " + authResponse.getAuthLeaseDuration());;
            } catch (VaultException e) {
                logger.warn("Failed to renew token!", e);
            }
        }
    }

    abstract class AbstractVaultClient implements VaultClientFactory{

        @NotNull
        private String addr;

        @NotNull
        private int engineVersion;


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

        public int getRenewInterval() { return renewInterval; }

        @JsonProperty
        public void setRenewInterval(int renewInterval) { this.renewInterval = renewInterval; }
    }

    class Token extends AbstractVaultClient{
        @NotNull
        private String token;

        @JsonProperty
        String getToken(){
            return token;
        }

        @JsonProperty
        public void setToken(String token){
            this.token = token;
        }
    }

    class Approle extends AbstractVaultClient{
        @NotNull
        private String roleId;

        @NotNull
        private String secretId;

        @JsonProperty
        public String getRoleId() {
            return roleId;
        }

        @JsonProperty
        public void setRoleId(String roleId) {
            this.roleId = roleId;
        }

        @JsonProperty
        public String getSecretId() {
            return secretId;
        }

        @JsonProperty
        public void setSecretId(String secretId) {
            this.secretId = secretId;
        }

        /**
         * Gets token by authenticating to Vault via AppRole: https://www.vaultproject.io/docs/auth/approle
         */
        @JsonProperty
        String getToken() throws VaultException {
            Vault initialVault = new Vault(
                    new VaultConfig()
                            .address(getAddr())
                            .build(), getEngineVersion());
            final AuthResponse response = initialVault.auth().loginByAppRole(roleId,
                    secretId);
            return response.getAuthClientToken();
        }
    }


}
