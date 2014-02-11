package io.keen.client.java;

import org.junit.BeforeClass;
import org.junit.Test;

import io.keen.client.java.exceptions.KeenException;


/**
 * KeenClientTest
 *
 * @author dkador
 * @since 1.0.0
 */
public class KeenClientTest {

    @BeforeClass
    public static void classSetUp() {
        KeenLogging.enableLogging();
    }

    @Test
    public void dummyTest() {
        // Do nothing.
    }

    /*
     TODO: Fix or remove shutdown tests.
    @Test
    public void testShutdown() throws KeenException, InterruptedException {
        getClient();

        assertFalse("Executor service should not be shutdown at the beginning of the test", KeenClient.EXECUTOR_SERVICE.isShutdown());

        KeenClient.shutdown(2000);

        assertTrue("Executor service should be shutdown at the end of the test", KeenClient.EXECUTOR_SERVICE.isShutdown());
        assertTrue("Executor service should be terminated at the end of the test", KeenClient.EXECUTOR_SERVICE.isTerminated());
    }
    */

    /**
     * It is important to have two shutdown tests so we test that the KeenClient recovers after a shutdown.
     * @throws KeenException
     * @throws InterruptedException
     */
    /*
     TODO: Fix or remove shutdown tests.
    @Test
    public void testShutdownNoWait() throws KeenException, InterruptedException {
        getClient();

        assertFalse("Executor service should not be shutdown at the beginning of the test", KeenClient.EXECUTOR_SERVICE.isShutdown());

        KeenClient.shutdown(0);

        assertTrue("Executor service should be shutdown at the end of the test", KeenClient.EXECUTOR_SERVICE.isShutdown());
        // Don't test termination here as it is a race condition whether the EXECUTOR_SERVICE will be terminated or not.
    }
     */

}
