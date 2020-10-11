package com.sproutsocial.nsqauthj.configuration;

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.response.AuthResponse;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import javax.validation.constraints.NotNull;

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

    abstract class AbstractVaultClient implements VaultClientFactory{

        @NotNull
        private String addr;

        @NotNull
        private int engineVersion;


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
            return new Vault(config, engineVersion);
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

    class Token extends AbstractVaultClient{
        @NotNull
        private String token;

        @JsonProperty
        String getToken(){
            return token;
        }

        @JsonProperty
        private void setToken(String token){
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
                            .build(), 1);
            final AuthResponse response = initialVault.auth().loginByAppRole(roleId,
                    secretId);
            return response.getAuthClientToken();
        }
    }


}
