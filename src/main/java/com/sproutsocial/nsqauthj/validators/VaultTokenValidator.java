package com.sproutsocial.nsqauthj.validators;

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.response.LogicalResponse;
import com.sproutsocial.nsqauthj.NsqAuthJConfiguration;
import com.sproutsocial.nsqauthj.configuration.TokenValidationFactory;
import com.sproutsocial.nsqauthj.tokens.NsqToken;

import javax.swing.text.html.Option;
import java.util.Arrays;
import java.util.Optional;

public class VaultTokenValidator {
    private final Vault vault;

    private final String userTokenPath;
    private final String serviceTokenPath;

    private final int ttl;

    public VaultTokenValidator(final Vault vault, NsqAuthJConfiguration configuration) {
        this.vault = vault;

        TokenValidationFactory tokenValidationFactory = configuration.getTokenValidationFactory();
        this.userTokenPath = tokenValidationFactory.getUserTokenPath();
        this.serviceTokenPath = tokenValidationFactory.getServiceTokenPath();
        this.ttl = tokenValidationFactory.getTokenTTL();
    }

    public VaultTokenValidator(Vault vault, String userTokenPath, String serviceTokenPath, int ttl) {
        this.vault = vault;
        this.userTokenPath = userTokenPath;
        this.serviceTokenPath = serviceTokenPath;
        this.ttl = ttl;
    }


    public Optional<NsqToken> validateTokenAtPath(String token, String path, NsqToken.TYPE type) {
        LogicalResponse response = null;
        try {
            response = this.vault.logical().read(path + token);
        } catch (VaultException e) {
            e.printStackTrace();
            return Optional.empty();
        }
        return NsqToken.fromVaultResponse(response, type, ttl);
    }

    public Optional<NsqToken> validateUserToken(String token) {
        return validateTokenAtPath(token, userTokenPath, NsqToken.TYPE.USER);
    }

    public Optional<NsqToken> validateServiceToken(String token) {
        return validateTokenAtPath(token, serviceTokenPath, NsqToken.TYPE.SERVICE);
    }

    public Optional<NsqToken> validateToken(String token) {
        // Check if this is a valid user token
        Optional<NsqToken> nsqToken;

        // It is far more likely we are dealing with a Service Token so check that first
        nsqToken = validateServiceToken(token);

        if (!nsqToken.isPresent()) {
            nsqToken = validateUserToken(token);
        }

        // If either is valid, we still want to allow publishing!
        // This is important as if Vault is having issues, we must still be able to publish messages!
        if (!nsqToken.isPresent()) {
           nsqToken = NsqToken.generatePublishOnlyToken(ttl);
        }
        return nsqToken;
    }
}
