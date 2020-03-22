package com.sproutsocial.nsq;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.powermock.api.mockito.PowerMockito.methods;
import static org.powermock.api.mockito.PowerMockito.suppress;

@RunWith(PowerMockRunner.class)
@PrepareForTest(SubConnection.class)
@PowerMockIgnore({"javax.management.*"})
public class SubConnectionInvalidHostTest {

    @Before
    public void beforeClass() {
        suppress(methods(SubConnection.class, "writeCommand"));
        suppress(methods(SubConnection.class, "flush"));
        suppress(methods(SubConnection.class, "scheduleAtFixedRate"));
    }

    @Test
    public void testConnectInvalidHostname() {
        ObservedConnectionClient client = new ObservedConnectionClient();
        try {
            Subscription subscription = Mockito.mock(Subscription.class);
            Mockito.when(subscription.getSubscriber())
                   .thenReturn(Mockito.mock(Subscriber.class));
            SubConnection subConnection = new SubConnection(client,
                    HostAndPort.fromString("this-is-unknown-hostname:5555"), subscription);

            Config config = Mockito.mock(Config.class);
            try {
                subConnection.connect(config);
                fail("IOException expected");
            } catch (IOException ok) {
            }
            finally {
                subConnection.close();
            }

            Thread.sleep(3000L);

            assertTrue(client.allSubConnectionsClosed());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            assertTrue(client.stop());
        }
    }
}
