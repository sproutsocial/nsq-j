package com.sproutsocial.nsqauthj.configuration;

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.response.AuthResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Automates periodic token renewal
 */
public class VaultTokenRenewer {

    private static Logger logger = LoggerFactory.getLogger(VaultTokenRenewer.class);

    private final String token;
    private final Vault vault;
    private final int ttl;


    /**
     * @param token     : The Vault authentication token used to establish a Vault connection
     * @param vault     : A Vault instance
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
            logger.info(authResponse.getTokenAccessor() + " renewed for " + authResponse.getAuthLeaseDuration());
            ;
        } catch (VaultException e) {
            logger.warn("Failed to renew token!", e);
        }
    }
}
