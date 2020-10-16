package com.sproutsocial.nsqauthj.validators;

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.api.Logical;
import com.bettercloud.vault.response.LogicalResponse;
import com.sproutsocial.nsqauthj.tokens.NsqToken;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.omg.PortableInterceptor.USER_EXCEPTION;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

class VaultTokenValidatorTest {

    private Vault mockVault;
    private VaultTokenValidator vaultTokenValidator;

    private final String token = "12345";
    private final String ip = "123.123.123.123";
    private final String serviceTokenPath ="secrets/nsq/service-tokens/";
    private final String userTokenPath ="secrets/nsq/user-tokens/";

    @BeforeEach
    void setUp() {
        mockVault = mock(Vault.class);
        vaultTokenValidator = Mockito.spy(new VaultTokenValidator(
                mockVault,
                userTokenPath,
                serviceTokenPath,
                300
        ));
    }

    @Test
    void validateTokenAtPathError() throws VaultException {
        Logical logicalMock = mock(Logical.class);
        when(mockVault.logical()).thenReturn(logicalMock);
        given(mockVault.logical().read(userTokenPath + token)).willAnswer(invocationOnMock -> { throw new VaultException("Garbage");});

        Optional<NsqToken> optionalNsqToken = vaultTokenValidator.validateTokenAtPath(token, userTokenPath, NsqToken.TYPE.USER, ip);

        assertFalse(optionalNsqToken.isPresent());
    }

    @Test
    void validateTokenAtPathValid() throws VaultException {
        LogicalResponse logicalResponseMock = mock(LogicalResponse.class);
        Map<String, String> responseData = new HashMap<>();
        responseData.put("username", "some.developer");
        responseData.put("topics", "tw_engagement,fb_post");
        when(logicalResponseMock.getData()).thenReturn(responseData);

        Logical logicalMock = mock(Logical.class);

        when(mockVault.logical()).thenReturn(logicalMock);
        when(mockVault.logical().read(userTokenPath + token)).thenReturn(logicalResponseMock);

        Optional<NsqToken> optionalNsqToken = vaultTokenValidator.validateTokenAtPath(token, userTokenPath, NsqToken.TYPE.USER, ip);

        assertTrue(optionalNsqToken.isPresent());
    }

    @Test
    void validateTokenUserToken() {
        Optional<NsqToken> nsqToken = Optional.of(new NsqToken(
                Arrays.asList(".*"),
                "some.developer",
                NsqToken.TYPE.USER,
                300,
                ip
        ));
        doReturn(Optional.empty()).when(vaultTokenValidator).validateServiceToken(token, ip);
        doReturn(nsqToken).when(vaultTokenValidator).validateUserToken(token, ip);

        Optional<NsqToken> returnedNsqToken = vaultTokenValidator.validateToken(token, ip);

        assertTrue(returnedNsqToken.isPresent());

        assertEquals(returnedNsqToken.get().getType(), NsqToken.TYPE.USER);
    }

    @Test
    void validateTokenServiceToken() {
        Optional<NsqToken> userToken = Optional.of(new NsqToken(
                Arrays.asList(".*"),
                "some.developer",
                NsqToken.TYPE.USER,
                300,
                ip
        ));
        Optional<NsqToken> serviceToken = Optional.of(new NsqToken(
                Arrays.asList(".*"),
                "a service",
                NsqToken.TYPE.SERVICE,
                300,
                ip
        ));
        doReturn(userToken).when(vaultTokenValidator).validateUserToken(token, ip);
        doReturn(serviceToken).when(vaultTokenValidator).validateServiceToken(token, ip);

        Optional<NsqToken> returnedNsqToken = vaultTokenValidator.validateToken(token, ip);

        assertTrue(returnedNsqToken.isPresent());

        assertEquals(returnedNsqToken.get().getType(), NsqToken.TYPE.SERVICE);
    }

    @Test
    void validateTokenPublishOnlyToken() {
        doReturn(Optional.empty()).when(vaultTokenValidator).validateServiceToken(token, ip);
        doReturn(Optional.empty()).when(vaultTokenValidator).validateUserToken(token, ip);

        Optional<NsqToken> returnedNsqToken = vaultTokenValidator.validateToken(token, ip);

        assertTrue(returnedNsqToken.isPresent());

        assertEquals(returnedNsqToken.get().getType(), NsqToken.TYPE.PUBLISH_ONLY);
    }
}