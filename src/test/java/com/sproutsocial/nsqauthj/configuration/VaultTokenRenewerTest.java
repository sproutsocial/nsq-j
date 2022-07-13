package com.sproutsocial.nsqauthj.configuration;

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.api.Auth;
import com.bettercloud.vault.response.AuthResponse;
import com.bettercloud.vault.response.LookupResponse;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class VaultTokenRenewerTest {

  Vault vault = mock(Vault.class);
  @Mock Auth auth;
  @Mock AuthResponse authResponse;
  @Mock LookupResponse lookupResponse;
  @Mock ScheduledExecutorService scheduledExecutorService;

  @BeforeEach
  void setUp() throws VaultException {
    when(vault.auth()).thenReturn(auth);
  }

  private VaultTokenRenewer createRenewer() throws VaultException {
    return new VaultTokenRenewer(vault, scheduledExecutorService, 10L);
  }

  /* does the first scheduled thing immediately on the calling thread, all future scheduled things are dropped */
  private void executeScheduledRunnableImmediatelyOnce() {
    when(scheduledExecutorService.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class))).thenAnswer((Answer<ScheduledFuture>) invocationOnMock -> {
      ScheduledFuture<?> theFuture = mock(ScheduledFuture.class);
      ((Runnable) invocationOnMock.getArgument(0)).run();
      return theFuture;
    }).thenReturn(mock(ScheduledFuture.class));
  }

  @Test
  void testRenewableToken() throws VaultException {
    // Arrange
    when(auth.lookupSelf()).thenReturn(lookupResponse);
    when(lookupResponse.isRenewable()).thenReturn(true);
    when(lookupResponse.getCreationTTL()).thenReturn(600L);
    when(scheduledExecutorService.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
            .thenReturn(mock(ScheduledFuture.class));

    // Act
    VaultTokenRenewer renewer = createRenewer();
    renewer.startRenewing();

    // Assert
    assertEquals(renewer.getRenewalIsScheduled(), true);
    assertEquals(renewer.getInterval(), 300L);
  }

  @Test
  void testNonRenewableToken() throws VaultException {
    // Arrange
    when(auth.lookupSelf()).thenReturn(lookupResponse);
    when(lookupResponse.isRenewable()).thenReturn(false);
    when(lookupResponse.getCreationTTL()).thenReturn(0L);

    // Act
    VaultTokenRenewer renewer = createRenewer();
    renewer.startRenewing();

    // Assert
    assertEquals(renewer.getRenewalIsScheduled(), false);
    assertEquals(renewer.getInterval(), 0L);
  }

  @Test
  void testTinyTTL() throws VaultException {
    // Arrange
    when(auth.lookupSelf()).thenReturn(lookupResponse);
    when(lookupResponse.isRenewable()).thenReturn(true);
    when(lookupResponse.getCreationTTL()).thenReturn(1L);

    // Act
    VaultTokenRenewer renewer = createRenewer();
    renewer.startRenewing();

    // Assert
    assertEquals(renewer.getRenewalIsScheduled(), false);
    assertEquals(renewer.getInterval(), 0L);
  }

  @Test
  void testFailedLookupInConstructor() throws VaultException {
    // Arrange
    when(auth.lookupSelf()).thenThrow(VaultException.class);

    // Act + Assert
    assertThrows(VaultException.class, this::createRenewer);
  }

  @Test
  void testFailedRenewal() throws VaultException {
    // Arrange
    when(auth.lookupSelf()).thenReturn(lookupResponse);
    when(lookupResponse.isRenewable()).thenReturn(true);
    when(lookupResponse.getCreationTTL()).thenReturn(600L);
    when(auth.renewSelf(anyLong())).thenThrow(VaultException.class);
    when(scheduledExecutorService.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
            .thenReturn(mock(ScheduledFuture.class));

    // Act
    VaultTokenRenewer renewer;
    renewer = createRenewer();
    renewer.renewToken();

    // Assert
    assertEquals(renewer.getRenewalIsScheduled(), true);
    assertEquals(renewer.getRenewalIsRetrying(), true);
  }

  @Test
  void testRenewalLookupBadToken() throws VaultException {
    // Arrange
    when(auth.lookupSelf())
      .thenReturn(lookupResponse)
      .thenThrow(new VaultException("bad-token", 403));
    when(lookupResponse.isRenewable()).thenReturn(true);
    when(lookupResponse.getCreationTTL()).thenReturn(600L);

    // Act
    VaultTokenRenewer renewer;
    renewer = createRenewer();
    renewer.renewToken();

    // Assert
    assertEquals(renewer.getRenewalIsScheduled(), false);
    assertEquals(renewer.getRenewalIsRetrying(), false);
  }

  @Test
  void testRenewalLookupOtherFailure() throws VaultException {
    // Arrange
    when(auth.lookupSelf())
      .thenReturn(lookupResponse)
      .thenThrow(new VaultException("other-exception", 500));
    when(auth.renewSelf(anyLong())).thenReturn(authResponse);
    when(authResponse.getAuthLeaseDuration()).thenReturn(600L);
    when(authResponse.getTokenAccessor()).thenReturn("FakeAccessor");
    when(lookupResponse.isRenewable()).thenReturn(true);
    when(lookupResponse.getCreationTTL()).thenReturn(600L);

    // Act
    VaultTokenRenewer renewer;
    renewer = createRenewer();
    renewer.renewToken();

    // Assert
    assertEquals(renewer.getRenewalIsScheduled(), false);
    assertEquals(renewer.getRenewalIsRetrying(), false);
  }

  @Test
  void testRetryImmediateState() throws VaultException, InterruptedException {
    // Arrange
    when(auth.lookupSelf()).thenReturn(lookupResponse);
    when(lookupResponse.isRenewable()).thenReturn(true);
    when(lookupResponse.getCreationTTL()).thenReturn(10L);
    when(auth.renewSelf(anyLong()))
      .thenThrow(VaultException.class)
      .thenReturn(authResponse);
    when(scheduledExecutorService.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
            .thenReturn(mock(ScheduledFuture.class));

    // Act
    VaultTokenRenewer renewer = createRenewer();
    renewer.renewToken();

    // Assert
    assertEquals(renewer.getRenewalIsScheduled(), true);
    assertEquals(renewer.getRenewalIsRetrying(), true);
  }

  @Test
  void testRetryAfterRetryCompletes() throws VaultException, InterruptedException {
    // Arrange
    when(auth.lookupSelf()).thenReturn(lookupResponse);
    when(authResponse.getTokenAccessor()).thenReturn("FakeAccessor");
    when(lookupResponse.isRenewable()).thenReturn(true);
    when(lookupResponse.getCreationTTL()).thenReturn(10L);
    when(auth.renewSelf(anyLong()))
            .thenThrow(VaultException.class)
            .thenReturn(authResponse);
    executeScheduledRunnableImmediatelyOnce();

    // Act
    VaultTokenRenewer renewer = createRenewer();
    renewer.renewToken();

    // Assert
    assertEquals(renewer.getRenewalIsScheduled(), true);
    assertEquals(renewer.getRenewalIsRetrying(), false);
  }
}
