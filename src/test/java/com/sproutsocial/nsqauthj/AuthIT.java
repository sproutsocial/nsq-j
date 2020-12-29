package com.sproutsocial.nsqauthj;

import com.bettercloud.vault.Vault;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.sproutsocial.nsqauthj.configuration.TokenValidationFactory;
import com.sproutsocial.nsqauthj.configuration.VaultClientFactory;
import com.sproutsocial.nsqauthj.permissions.NsqPermissionSet;
import com.sproutsocial.nsqauthj.resources.AuthResource;
import com.sproutsocial.nsqauthj.validators.VaultTokenValidator;
import com.sproutsocial.platform.Heartbeater;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(DropwizardExtensionsSupport.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AuthIT {

    ResourceExtension resourceExtension;

    @BeforeAll
    public void setUp() throws Exception {
        TokenValidationFactory tokenValidationFactory = new TokenValidationFactory();
        tokenValidationFactory.setServiceTokenPath("secret/services/nsq/service-tokens/");
        tokenValidationFactory.setUserTokenPath("secret/services/nsq/user-tokens/");
        tokenValidationFactory.setTokenTTL(3600);

        VaultClientFactory.Token vaultClientFactory = new VaultClientFactory.Token();
        vaultClientFactory.setToken("root");
        vaultClientFactory.setAddr("http://127.0.0.1:8200");
        vaultClientFactory.setEngineVersion(2);

        Vault vault = vaultClientFactory.build();
        MetricRegistry metricRegistry = mock(MetricRegistry.class);
        when(metricRegistry.counter(anyString())).thenReturn(new Counter());

        VaultTokenValidator vaultTokenValidator = tokenValidationFactory.build(vault, metricRegistry);

        Heartbeater heartbeater = mock(Heartbeater.class);

        resourceExtension = ResourceExtension
                .builder()
                .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
                .addResource(new AuthResource(vaultTokenValidator, heartbeater, metricRegistry))
                .build();
    }

    @Test
    public void testUserToken() {
        final Response response = resourceExtension
                .target("/auth")
                .queryParam("secret", "1234")
                .request(MediaType.APPLICATION_JSON)
                .get();
        NsqPermissionSet nsqPermissionSet = response.readEntity(NsqPermissionSet.class);
        assertEquals(200, response.getStatus());
        assertEquals("example_user_token", nsqPermissionSet.getIdentity());
        for (NsqPermissionSet.Authorization authorization : nsqPermissionSet.getAuthorizations()) {
            assertEquals(Arrays.asList("subscribe", "publish"), authorization.getPermissions());
        }
    }

    @Test
    public void testServiceToken() {
        final Response response = resourceExtension
                .target("/auth")
                .queryParam("secret", "1234")
                .request(MediaType.APPLICATION_JSON)
                .get();
        NsqPermissionSet nsqPermissionSet = response.readEntity(NsqPermissionSet.class);
        assertEquals(200, response.getStatus());
        assertEquals("example_user_token", nsqPermissionSet.getIdentity());
        for (NsqPermissionSet.Authorization authorization : nsqPermissionSet.getAuthorizations()) {
            assertEquals(Arrays.asList("subscribe", "publish"), authorization.getPermissions());
        }
    }

    @Test
    public void testPublishOnlyToken() {
        final Response response = resourceExtension
                .target("/auth")
                .queryParam("secret", "garbage")
                .request(MediaType.APPLICATION_JSON)
                .get();
        NsqPermissionSet nsqPermissionSet = response.readEntity(NsqPermissionSet.class);
        assertEquals(200, response.getStatus());
        assertEquals("127.0.0.1", nsqPermissionSet.getIdentity());
        for (NsqPermissionSet.Authorization authorization : nsqPermissionSet.getAuthorizations()) {
            assertEquals(Arrays.asList("publish"), authorization.getPermissions());
        }
    }
}
