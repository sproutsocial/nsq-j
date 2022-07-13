package com.sproutsocial.nsqauthj.configuration;

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.response.AuthResponse;
import com.bettercloud.vault.response.LookupResponse;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Automates periodic token renewal
 */
public class VaultTokenRenewer {

    private static Logger logger = LoggerFactory.getLogger(VaultTokenRenewer.class);

    private final ScheduledExecutorService scheduledExecutorService;
    private final Vault vault;
    private final long ttl;
    private final long interval;
    private final long retryDelay;
    private final boolean renewable;

    private boolean retrying = false;
    private ScheduledFuture<?> scheduledRenewals = null;

    /**
     * @param vault     : A Vault instance
     */
    public VaultTokenRenewer(Vault vault) throws VaultException {
        this(vault, Executors.newSingleThreadScheduledExecutor(), 10L);
    }

    /**
     * @param vault : A Vault instance
     * @param minRetryDelay : Smallest allowed delay (in seconds) between token renewal retries
     * This is allowed to throw VaultException, since failing to parse the token
     * breaks the instance's ability to initialize.
     */
    public VaultTokenRenewer(Vault vault, ScheduledExecutorService scheduledExecutorService, long minRetryDelay) throws VaultException {
        this.vault = vault;
        this.scheduledExecutorService = scheduledExecutorService;

        final LookupResponse tokenLookup = vault.auth().lookupSelf();
        this.ttl = tokenLookup.getCreationTTL();
        this.interval = this.ttl / 2;
        this.renewable = tokenLookup.isRenewable();

        // Retry after interval / 20, or minRetryDelay, whichever is smaller
        // Interval is normally TTL/2, so interval/20 will retry at
        // least 19 times before the token expires. If TTL is
        // actually under 400s ... why?
        this.retryDelay = interval / 20 < minRetryDelay ? minRetryDelay : interval / 20;

    }

    /**
     * Makes the call to renew the token
     */
    public boolean renewToken() {
        try {
            // Ensure this token is still valid at all.
            vault.auth().lookupSelf();
        }
        catch (VaultException e) {
            // If this failed due to an auth failure it means the
            // token expired (or was never valid) and will never be renewed
            if (e.getHttpStatusCode() == 403) {
                logger.error("Expired/invalid tokens cannot be renewed!", e);
                return false;
            }
        }

        try {
            AuthResponse authResponse = vault.auth().renewSelf(ttl);
            if (retrying) {
                // Cancel the existing future
                stopRenewing();
                // Start a new future with the regular interval
                startRenewing(interval);
                retrying = false;
            }
            logger.info("{} renewed for {}s", authResponse.getTokenAccessor(), authResponse.getAuthLeaseDuration());
            return true;
        } catch (VaultException e) {
            logger.warn("Failed to renew token!", e);
            logger.info("Will retry token renewal in {}s", retryDelay);
            if (!retrying) {
                // Cancel the existing future
                stopRenewing();
                // Start a new future with the retry interval
                retrying = true;
                startRenewing(retryDelay);
            }
        }
        return false;
    }

    protected void startRenewing() {
        startRenewing(interval);
    }

    protected boolean startRenewing(long delay) {
        if (renewable && interval > 0 && scheduledRenewals == null) {
            if (!retrying) {
                logger.info("Scheduling token renewal every {}s", interval);
            }
            scheduledRenewals = scheduledExecutorService.scheduleAtFixedRate(
                    this::renewToken,
                    delay < 0 ? 0 : delay,
                    interval,
                    TimeUnit.SECONDS
            );
            return true;
        }
        logger.warn("Vault token is not renewable or TTL/2 is zero");
        return false;
    }

    protected void stopRenewing() {
        if (scheduledRenewals != null) {
            scheduledRenewals.cancel(false);
            scheduledRenewals = null;
        }
    }

    protected boolean getTokenIsRenewable() {
        return renewable;
    }

    protected boolean getRenewalIsScheduled() {
        return scheduledRenewals != null;
    }

    protected boolean getRenewalIsRetrying() {
        return retrying;
    }

    protected long getTTL() {
        return ttl;
    }

    protected long getInterval() {
        return interval;
    }
}
