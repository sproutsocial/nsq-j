package com.sproutsocial.nsqauthj.configuration;

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.response.AuthResponse;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import javax.validation.constraints.NotNull;

public class JwtClientFactory extends AbstractVaultClientFactory {

    private static String JWT_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token";

    @NotNull
    private String authPath;

    @NotNull
    private String role;

    @JsonProperty
    public String getAuthPath() {
        return authPath;
    }

    @JsonProperty
    public void setAuthPath(String authPath) {
        this.authPath = authPath;
    }

    @JsonProperty
    public String getRole() {
        return role;
    }

    @JsonProperty
    public void setRole(String role) {
        this.role = role;
    }


    String getToken() throws VaultException {
        final String jwt = getJwt();
        Vault initialVault = new Vault(
                new VaultConfig()
                        .address(getAddr())
                        .build(), getEngineVersion());
        final AuthResponse response = initialVault.auth().loginByJwt(authPath, role, jwt);
        return response.getAuthClientToken();
    }

    /**
     * Helper method to read the JWT token from a kubernetes pod. Every kubernetes pod has this and
     * is found at the path `/var/run/secrets/kubernetes.io/serviceaccount/token`. This JWT Token
     * is based on the serviceaccount specified at pod startup and links the Vault Auth Backend to
     * the pod.
     */
    private static String getJwt() {
        try {
            return new String(Files.readAllBytes(Paths.get(JWT_PATH)));
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Could not read JWT Token from Kubernetes pod at path: %s: %s", JWT_PATH, e)
            );
        }
    }
}
