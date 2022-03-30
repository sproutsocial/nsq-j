package com.sproutsocial.nsqauthj.configuration;

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.response.AuthResponse;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.constraints.NotNull;

public class ApproleClientFactory extends AbstractVaultClientFactory {
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
    @JsonIgnore
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
