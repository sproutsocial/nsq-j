package com.sproutsocial.nsqauthj.validators;

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.response.LogicalResponse;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.sproutsocial.nsqauthj.NsqAuthJConfiguration;
import com.sproutsocial.nsqauthj.configuration.TokenValidationFactory;
import com.sproutsocial.nsqauthj.tokens.NsqToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class VaultTokenValidator {
    private static final Logger logger = LoggerFactory.getLogger(VaultTokenValidator.class);
    private final Vault vault;

    private final String userTokenPath;
    private final String serviceTokenPath;

    private final int ttl;

    private final Counter serviceCounter;
    private final Counter userCounter;
    private final Counter publishOnlyCounter;

    public VaultTokenValidator(Vault vault, String userTokenPath, String serviceTokenPath, int ttl, MetricRegistry metricRegistry) {
        this.vault = vault;
        this.userTokenPath = userTokenPath;
        this.serviceTokenPath = serviceTokenPath;
        this.ttl = ttl;
        serviceCounter = metricRegistry.counter("granted.vault.service");
        userCounter = metricRegistry.counter("granted.vault.user");
        publishOnlyCounter = metricRegistry.counter("granted.failed.publish_only");
    }


    public Optional<NsqToken> validateTokenAtPath(String token, String path, NsqToken.TYPE type, String remoteAddr) {
        LogicalResponse response = null;
        try {
            response = this.vault.logical().read(path + token);
        } catch (VaultException e) {
            e.printStackTrace();
            return Optional.empty();
        }
        return NsqToken.fromVaultResponse(response, token, type, ttl, remoteAddr);
    }

    public Optional<NsqToken> validateUserToken(String token, String remoteAddr) {
        return validateTokenAtPath(token, userTokenPath, NsqToken.TYPE.USER, remoteAddr);
    }

    public Optional<NsqToken> validateServiceToken(String token, String remoteAddr) {
        return validateTokenAtPath(token, serviceTokenPath, NsqToken.TYPE.SERVICE, remoteAddr);
    }

    public Optional<NsqToken> validateToken(String token, String remoteAddr) {
        // Check if this is a valid user token
        Optional<NsqToken> nsqToken;

        // It is far more likely we are dealing with a Service Token so check that first
        nsqToken = validateServiceToken(token, remoteAddr);

        if (nsqToken.isPresent()) {
            serviceCounter.inc();
            return nsqToken;
        }

        // Check if it is a user token
        nsqToken = validateUserToken(token, remoteAddr);
        if (nsqToken.isPresent()) {
            userCounter.inc();
            return nsqToken;
        }

        // If either is invalid, we still want to allow publishing!
        // This is important as if Vault is having issues, we must still be able to publish messages!
        logger.warn("Unable to find User or Service token for provided token " + token + " from " + remoteAddr);
        publishOnlyCounter.inc();
        return NsqToken.generatePublishOnlyToken(ttl, remoteAddr);
    }
}
