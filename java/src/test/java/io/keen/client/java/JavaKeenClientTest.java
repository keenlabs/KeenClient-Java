package io.keen.client.java;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Tests the JavaKeenClient class.
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public class JavaKeenClientTest {

    private static KeenProject TEST_PROJECT;

    private KeenClient client;

    @BeforeClass
    public static void classSetUp() {
        KeenLogging.enableLogging();
        JavaKeenClient.initialize();
        TEST_PROJECT = new KeenProject("508339b0897a2c4282000000",
                "80ce00d60d6443118017340c42d1cfaf",
                "80ce00d60d6443118017340c42d1cfaf");
    }

    @Before
    public void setUp() {
        client = KeenClient.client();
        client.setBaseUrl(null);
        client.setDebugMode(true);
    }

    @After
    public void cleanUp() {
        client = null;
    }

    @Test
    public void testAddEvent() throws Exception {
        // does a full round-trip to the real API.
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("test key", "test value");
        // setup a latch for our callback so we can verify the server got the request
        final CountDownLatch latch = new CountDownLatch(1);
        // send the event
        client.addEvent(TEST_PROJECT, "foo", event, null, new LatchKeenCallback(latch));
        // make sure the event was sent to Keen IO
        latch.await(2, TimeUnit.SECONDS);
    }

    @Test
    public void testAddEventNonSSL() throws Exception {
        // does a full round-trip to the real API.
        client.setBaseUrl("http://api.keen.io");
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("test key", "test value");
        // setup a latch for our callback so we can verify the server got the request
        final CountDownLatch latch = new CountDownLatch(1);
        // send the event
        client.addEvent(TEST_PROJECT, "foo", event, null, new LatchKeenCallback(latch));
        // make sure the event was sent to Keen IO
        latch.await(2, TimeUnit.SECONDS);
    }

    @Test
    public void testShutdown() throws Exception {
        ((JavaKeenClient) client).restartPublishExecutorService();
        ExecutorService service = (ExecutorService) client.getPublishExecutor();
        assertFalse("Executor service should not be shutdown at the beginning of the test", service.isShutdown());

        service.shutdown();
        service.awaitTermination(2000, TimeUnit.MILLISECONDS);

        assertTrue("Executor service should be shutdown at the end of the test", service.isShutdown());
        assertTrue("Executor service should be terminated at the end of the test", service.isTerminated());
    }

    /**
     * It is important to have two shutdown tests so we test that the KeenClient recovers after a shutdown.
     */
    @Test
    public void testShutdownNoWait() throws Exception {
        ((JavaKeenClient) client).restartPublishExecutorService();
        ExecutorService service = (ExecutorService) client.getPublishExecutor();
        assertFalse("Executor service should not be shutdown at the beginning of the test", service.isShutdown());

        service.shutdownNow();

        assertTrue("Executor service should be shutdown at the end of the test", service.isShutdown());
        // Don't test termination here as it is a race condition whether the EXECUTOR_SERVICE will be terminated or not.
    }

    private static class LatchKeenCallback implements KeenCallback {

        private final CountDownLatch latch;

        LatchKeenCallback(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onSuccess() {
            latch.countDown();
        }

        @Override
        public void onFailure(Exception e) {
        }
    }

}
