package com.sproutsocial.nsqauthj.validators;

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.response.LogicalResponse;
import com.bettercloud.vault.rest.RestException;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.sproutsocial.nsqauthj.tokens.NsqToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketTimeoutException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class VaultTokenValidator {
    private static final Logger logger = LoggerFactory.getLogger(VaultTokenValidator.class);
    private final Vault vault;

    private final String userTokenPath;
    private final String serviceTokenPath;

    private final int ttl;

    private Boolean failOpen;

    private final Counter serviceCounter;
    private final Counter userCounter;
    private final Counter publishOnlyCounter;

    private static final int MAX_RETRIES = 3;
    private static final int RETRY_DELAY = 200; // time before retrying in ms

    public VaultTokenValidator(Vault vault, String userTokenPath, String serviceTokenPath,
                               int ttl, Boolean failOpen, MetricRegistry metricRegistry) {
        this.vault = vault;
        this.userTokenPath = userTokenPath;
        this.serviceTokenPath = serviceTokenPath;
        this.ttl = ttl;
        this.failOpen = failOpen;
        serviceCounter = metricRegistry.counter("granted.vault.service");
        userCounter = metricRegistry.counter("granted.vault.user");
        publishOnlyCounter = metricRegistry.counter("granted.failed.publish_only");
    }


    public Optional<NsqToken> validateTokenAtPath(String token, String path, NsqToken.TYPE type, String remoteAddr) {
        for (int i = 1; i <= MAX_RETRIES; i++) {
            try {
                LogicalResponse response = this.vault.logical().read(path + token);
                return NsqToken.fromVaultResponse(response, token, type, ttl, remoteAddr);
            } catch (VaultException e) {
                if (isTimeout(e)) {
                    logger.warn("Timed out reading from Vault, retrying...", e);
                    wait(200, TimeUnit.MILLISECONDS);
                    continue;
                }
                logger.warn(e.getMessage(), e);
                break;
            }
        }
        return Optional.empty();
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

    public Boolean getFailOpen() {
        return failOpen;
    }

    private static boolean isTimeout(VaultException e) {
        return e.getCause() != null
                && e.getCause() instanceof RestException
                && e.getCause().getCause() != null
                && e.getCause().getCause() instanceof SocketTimeoutException;
    }



    private void wait(int value, TimeUnit unit) {
        try {
            Thread.sleep(unit.toMillis(value));
        } catch (InterruptedException e) {
            logger.error("Thread interrupted while sleeping");
        }
    }
}
