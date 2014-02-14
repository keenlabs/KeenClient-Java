package io.keen.client.java;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import io.keen.client.java.exceptions.KeenException;
import io.keen.client.java.exceptions.NoWriteKeyException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;


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
    public void testEnvironment() {
        try {
            KeenClient.client();
            fail("Shouldn't be able to get client if no environment set.");
        } catch (IllegalStateException e) {
        }

        try {
            KeenClient.createStaticInstance(getEnvironment(null, null, null));
            KeenClient.client();
            fail("Shouldn't be able to get client if bad environment used.");
        } catch (IllegalStateException e) {
        }

        try {
            KeenClient.createStaticInstance(getEnvironment(null, "abc", "def"));
            KeenClient.client();
            fail("Shouldn't be able to get client if no project id in environment.");
        } catch (IllegalStateException e) {
        }

        KeenClient.createStaticInstance(getEnvironment("project_id", "abc", "def"));
        doClientAssertions("project_id", "abc", "def", KeenClient.client());

        KeenClient.clearStaticInstance();
    }

    private Environment getEnvironment(final String projectId, final String writeKey, final String readKey) {
        return new Environment() {
            @Override
            public String getKeenProjectId() {
                return projectId;
            }

            @Override
            public String getKeenWriteKey() {
                return writeKey;
            }

            @Override
            public String getKeenReadKey() {
                return readKey;
            }
        };
    }

    @Test
    public void testKeenClientConstructor() {
        runKeenClientConstructorTest(null, null, null, true, "null project id", "Invalid project id specified: null");
        runKeenClientConstructorTest("", null, null, true, "empty project id", "Invalid project id specified: ");
        runKeenClientConstructorTest("abc", "def", "ghi", false, "everything is good", null);
    }

    private void runKeenClientConstructorTest(String projectId, String writeKey, String readKey, boolean shouldFail,
                                              String msg, String expectedMessage) {
        try {
            KeenClient client = new KeenClient(projectId, writeKey, readKey);
            if (shouldFail) {
                fail(msg);
            } else {
                doClientAssertions(projectId, writeKey, readKey, client);
            }
        } catch (IllegalArgumentException e) {
            assertEquals(expectedMessage, e.getLocalizedMessage());
        }
    }

    @Test
    public void testSharedClient() {
        // can't get client without first initializing it
        try {
            KeenClient.client();
            fail("can't get client without first initializing it");
        } catch (IllegalStateException e) {
        }

        // make sure bad values error correctly
        try {
            KeenClient.createStaticInstance(null, null, null);
            fail("can't use bad values");
        } catch (IllegalArgumentException e) {
        }

        KeenClient.createStaticInstance("abc", "def", "ghi");
        KeenClient client = KeenClient.client();
        doClientAssertions("abc", "def", "ghi", client);
    }

    @Test
    public void testInvalidEventCollection() throws KeenException {
        runValidateAndBuildEventTest(TestUtils.getSimpleEvent(), "$asd", "collection can't start with $",
                                     "An event collection name cannot start with the dollar sign ($) character.");

        String tooLong = TestUtils.getString(257);
        runValidateAndBuildEventTest(TestUtils.getSimpleEvent(), tooLong, "collection can't be longer than 256 chars",
                                     "An event collection name cannot be longer than 256 characters.");
    }

    @Test
    public void testValidateAndBuildEvent() throws KeenException, IOException {
        runValidateAndBuildEventTest(null, "foo", "null event",
                                     "You must specify a non-null, non-empty event.");

        runValidateAndBuildEventTest(new HashMap<String, Object>(), "foo", "empty event",
                                     "You must specify a non-null, non-empty event.");

        Map<String, Object> event = new HashMap<String, Object>();
        event.put("keen", "reserved");
        runValidateAndBuildEventTest(event, "foo", "keen reserved",
                                     "An event cannot contain a root-level property named 'keen'.");

        event.remove("keen");
        event.put("ab.cd", "whatever");
        runValidateAndBuildEventTest(event, "foo", ". in property name",
                                     "An event cannot contain a property with the period (.) character in it.");

        event.remove("ab.cd");
        event.put("$a", "whatever");
        runValidateAndBuildEventTest(event, "foo", "$ at start of property name",
                                     "An event cannot contain a property that starts with the dollar sign ($) " +
                                             "character in it.");

        event.remove("$a");
        String tooLong = TestUtils.getString(257);
        event.put(tooLong, "whatever");
        runValidateAndBuildEventTest(event, "foo", "too long property name",
                                     "An event cannot contain a property name longer than 256 characters.");

        event.remove(tooLong);
        tooLong = TestUtils.getString(10000);
        event.put("long", tooLong);
        runValidateAndBuildEventTest(event, "foo", "too long property value",
                                     "An event cannot contain a string property value longer than 10,000 characters.");

        // now do a basic add
        event.remove("long");
        event.put("valid key", "valid value");
        KeenClient client = getClient();
        Map<String, Object> builtEvent = client.validateAndBuildEvent("foo", event, null);
        assertNotNull(builtEvent);
        assertEquals("valid value", builtEvent.get("valid key"));
        // also make sure the event has been timestamped
        @SuppressWarnings("unchecked")
        Map<String, Object> keenNamespace = (Map<String, Object>) builtEvent.get("keen");
        assertNotNull(keenNamespace);
        assertNotNull(keenNamespace.get("timestamp"));

        // an event with a Calendar should work
        Calendar now = Calendar.getInstance();
        event.put("datetime", now);
        client.validateAndBuildEvent("foo", event, null);

        // an event with a nested property called "keen" should work
        event = TestUtils.getSimpleEvent();
        Map<String, Object> nested = new HashMap<String, Object>();
        nested.put("keen", "value");
        event.put("nested", nested);
        client.validateAndBuildEvent("foo", event, null);
    }

    @Test
    public void testAddEventNoWriteKey() throws KeenException, IOException {
        // TODO: Don't special-case using debug mode here.
        KeenClient.setDebugMode(true);
        KeenClient client = getClient("508339b0897a2c4282000000", null, null);
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("test key", "test value");
        try {
            client.addEvent("foo", event);
            fail("add event without write key should fail");
        } catch (NoWriteKeyException e) {
            assertEquals("You can't send events to Keen IO if you haven't set a write key.",
                    e.getLocalizedMessage());
        } finally {
            KeenClient.setDebugMode(false);
        }
    }

    /*
     * TODO: Re-implement these tests with a mock JSON handler, or move them to the java
     * package.
    @Test
    public void testAddEvent() throws KeenException, IOException, InterruptedException {
        // does a full round-trip to the real API.
        KeenClient client = getClient();
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("test key", "test value");
        // setup a latch for our callback so we can verify the server got the request
        final CountDownLatch latch = new CountDownLatch(1);
        // send the event
        client.addEvent("foo", event, null, new AddEventCallback() {
            @Override
            public void onSuccess() {
                latch.countDown();
            }

            @Override
            public void onError(String responseBody) {
            }
        });
        // make sure the event was sent to Keen IO
        latch.await(2, TimeUnit.SECONDS);
    }

    @Test
    public void testAddEventNonSSL() throws KeenException, IOException, InterruptedException {
        // does a full round-trip to the real API.
        KeenClient client = getClient();
        client.setBaseUrl("http://api.keen.io");
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("test key", "test value");
        // setup a latch for our callback so we can verify the server got the request
        final CountDownLatch latch = new CountDownLatch(1);
        // send the event
        client.addEvent("foo", event, null, new AddEventCallback() {
            @Override
            public void onSuccess() {
                latch.countDown();
            }

            @Override
            public void onError(String responseBody) {
            }
        });
        // make sure the event was sent to Keen IO
        latch.await(2, TimeUnit.SECONDS);
    }
    */

    /*
     TODO: Fix or get rid of this test.
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
     TODO: Fix or get rid of this test.
    @Test
    public void testShutdownNoWait() throws KeenException, InterruptedException {
        getClient();

        assertFalse("Executor service should not be shutdown at the beginning of the test", KeenClient.EXECUTOR_SERVICE.isShutdown());

        KeenClient.shutdown(0);

        assertTrue("Executor service should be shutdown at the end of the test", KeenClient.EXECUTOR_SERVICE.isShutdown());
        // Don't test termination here as it is a race condition whether the EXECUTOR_SERVICE will be terminated or not.
    }
    */

    /*
     TODO: Fix or get rid of these tests.
    @Test
    public void testRequestHandling() throws Exception {
        InputStream stream = new ByteArrayInputStream("blah".getBytes("UTF-8"));

        // if there's no callback, this shouldn't do anything (except log), regardless of status code
        KeenHttpRequestRunnable.handleResult(stream, 201, null);
        KeenHttpRequestRunnable.handleResult(stream, 200, null);
        KeenHttpRequestRunnable.handleResult(stream, 500, null);

        // if there's a callback, this SHOULD do something
        runRequestHandlerTest("blah", 201, null);
        runRequestHandlerTest("blah", 200, "blah");
        runRequestHandlerTest("blah", 400, "blah");
        runRequestHandlerTest("blah", 500, "blah");
    }

    private void runRequestHandlerTest(String response, int statusCode, String expectedError) throws Exception {
        InputStream stream = new ByteArrayInputStream(response.getBytes("UTF-8"));
        final CountDownLatch successLatch = new CountDownLatch(1);
        final CountDownLatch errorLatch = new CountDownLatch(1);
        MyCallback callback = new MyCallback(successLatch, errorLatch);
        KeenHttpRequestRunnable.handleResult(stream, statusCode, callback);
        if (expectedError == null) {
            successLatch.await(10, TimeUnit.MILLISECONDS);
            assertEquals(1, errorLatch.getCount());
        } else {
            errorLatch.await(10, TimeUnit.MILLISECONDS);
            assertEquals(0, errorLatch.getCount());
            assertEquals(expectedError, callback.errorResponse);
        }
    }
    */

    class MyCallback implements KeenCallback {
        final CountDownLatch successLatch;
        final CountDownLatch errorLatch;
        Exception e;

        MyCallback(CountDownLatch successLatch, CountDownLatch errorLatch) {
            this.successLatch = successLatch;
            this.errorLatch = errorLatch;
        }

        @Override
        public void onSuccess() {
            successLatch.countDown();
        }

        @Override
        public void onFailure(Exception e) {
            this.e = e;
            errorLatch.countDown();
        }
    }

    @Test
    public void testGlobalPropertiesMap() throws Exception {
        // a null map should be okay
        runGlobalPropertiesMapTest(null, 1);

        // an empty map should be okay
        runGlobalPropertiesMapTest(new HashMap<String, Object>(), 1);

        // a map w/ non-conflicting property names should be okay
        Map<String, Object> globals = new HashMap<String, Object>();
        globals.put("default name", "default value");
        Map<String, Object> builtEvent = runGlobalPropertiesMapTest(globals, 2);
        assertEquals("default value", builtEvent.get("default name"));

        // a map that returns a conflicting property name should not overwrite the property on the event
        globals = new HashMap<String, Object>();
        globals.put("a", "c");
        builtEvent = runGlobalPropertiesMapTest(globals, 1);
        assertEquals("b", builtEvent.get("a"));
    }

    private Map<String, Object> runGlobalPropertiesMapTest(Map<String, Object> globalProperties,
                                                           int expectedNumProperties) throws Exception {
        KeenClient client = getClient();
        client.setGlobalProperties(globalProperties);
        Map<String, Object> event = TestUtils.getSimpleEvent();
        String eventCollection = String.format("foo%d", Calendar.getInstance().getTimeInMillis());
        Map<String, Object> builtEvent = client.validateAndBuildEvent(eventCollection, event, null);
        assertEquals(expectedNumProperties + 1, builtEvent.size());
        return builtEvent;
    }

    @Test
    public void testGlobalPropertiesEvaluator() throws Exception {
        // a null evaluator should be okay
        runGlobalPropertiesEvaluatorTest(null, 1);

        // an evaluator that returns an empty map should be okay
        GlobalPropertiesEvaluator evaluator = new GlobalPropertiesEvaluator() {
            @Override
            public Map<String, Object> getGlobalProperties(String eventCollection) {
                return new HashMap<String, Object>();
            }
        };
        runGlobalPropertiesEvaluatorTest(evaluator, 1);

        // an evaluator that returns a map w/ non-conflicting property names should be okay
        evaluator = new GlobalPropertiesEvaluator() {
            @Override
            public Map<String, Object> getGlobalProperties(String eventCollection) {
                Map<String, Object> globals = new HashMap<String, Object>();
                globals.put("default name", "default value");
                return globals;
            }
        };
        Map<String, Object> builtEvent = runGlobalPropertiesEvaluatorTest(evaluator, 2);
        assertEquals("default value", builtEvent.get("default name"));

        // an evaluator that returns a map w/ conflicting property name should not overwrite the property on the event
        evaluator = new GlobalPropertiesEvaluator() {
            @Override
            public Map<String, Object> getGlobalProperties(String eventCollection) {
                Map<String, Object> globals = new HashMap<String, Object>();
                globals.put("a", "c");
                return globals;
            }
        };
        builtEvent = runGlobalPropertiesEvaluatorTest(evaluator, 1);
        assertEquals("b", builtEvent.get("a"));
    }

    private Map<String, Object> runGlobalPropertiesEvaluatorTest(GlobalPropertiesEvaluator evaluator,
                                                                 int expectedNumProperties) throws Exception {
        KeenClient client = getClient();
        client.setGlobalPropertiesEvaluator(evaluator);
        Map<String, Object> event = TestUtils.getSimpleEvent();
        String eventCollection = String.format("foo%d", Calendar.getInstance().getTimeInMillis());
        Map<String, Object> builtEvent = client.validateAndBuildEvent(eventCollection, event, null);
        assertEquals(expectedNumProperties + 1, builtEvent.size());
        return builtEvent;
    }

    @Test
    public void testGlobalPropertiesTogether() throws Exception {
        KeenClient client = getClient();

        // properties from the evaluator should take precedence over properties from the map
        // but properties from the event itself should take precedence over all
        Map<String, Object> globalProperties = new HashMap<String, Object>();
        globalProperties.put("default property", 5);
        globalProperties.put("foo", "some new value");

        GlobalPropertiesEvaluator evaluator = new GlobalPropertiesEvaluator() {
            @Override
            public Map<String, Object> getGlobalProperties(String eventCollection) {
                Map<String, Object> map = new HashMap<String, Object>();
                map.put("default property", 6);
                map.put("foo", "some other value");
                return map;
            }
        };

        client.setGlobalProperties(globalProperties);
        client.setGlobalPropertiesEvaluator(evaluator);

        Map<String, Object> event = new HashMap<String, Object>();
        event.put("foo", "bar");
        Map<String, Object> builtEvent = client.validateAndBuildEvent("apples", event, null);

        assertEquals("bar", builtEvent.get("foo"));
        assertEquals(6, builtEvent.get("default property"));
        assertEquals(3, builtEvent.size());
    }

    private void runValidateAndBuildEventTest(Map<String, Object> event, String eventCollection, String msg,
                                              String expectedMessage) {
        KeenClient client = getClient();
        try {
            client.validateAndBuildEvent(eventCollection, event, null);
            fail(msg);
        } catch (KeenException e) {
            assertEquals(expectedMessage, e.getLocalizedMessage());
        }
    }

    private void doClientAssertions(String expectedProjectId, String expectedWriteKey,
                                    String expectedReadKey, KeenClient client) {
        assertEquals(expectedProjectId, client.getProjectId());
        assertEquals(expectedWriteKey, client.getWriteKey());
        assertEquals(expectedReadKey, client.getReadKey());
    }

    private KeenClient getClient() {
        return getClient("508339b0897a2c4282000000", "80ce00d60d6443118017340c42d1cfaf",
                         "80ce00d60d6443118017340c42d1cfaf");
    }

    private KeenClient getClient(String projectId, String writeKey, String readKey) {
        return new KeenClient(projectId, writeKey, readKey);
    }

}
