package com.sproutsocial.nsq;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for AuthFailedException
 */
public class AuthFailedExceptionTest {

    @Test
    public void testAuthFailedExceptionWithMessage() {
        String errorMessage = "E_AUTH_FAILED auth session expired";
        AuthFailedException exception = new AuthFailedException(errorMessage);

        Assert.assertEquals(errorMessage, exception.getMessage());
        Assert.assertNull(exception.getCause());
        Assert.assertTrue(exception instanceof NSQException);
    }

    @Test
    public void testAuthFailedExceptionWithMessageAndCause() {
        String errorMessage = "E_UNAUTHORIZED unauthorized access";
        RuntimeException cause = new RuntimeException("Connection error");
        AuthFailedException exception = new AuthFailedException(errorMessage, cause);

        Assert.assertEquals(errorMessage, exception.getMessage());
        Assert.assertEquals(cause, exception.getCause());
        Assert.assertTrue(exception instanceof NSQException);
    }

    @Test
    public void testAuthFailedExceptionInheritance() {
        AuthFailedException exception = new AuthFailedException("test");

        // Verify it's properly extending NSQException
        Assert.assertTrue("AuthFailedException should extend NSQException",
                         exception instanceof NSQException);
        Assert.assertTrue("AuthFailedException should extend RuntimeException",
                         exception instanceof RuntimeException);
    }
}
