package io.keen.client.java;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.keen.client.java.exceptions.KeenException;
import io.keen.client.java.exceptions.KeenInitializationException;
import io.keen.client.java.exceptions.NoWriteKeyException;
import io.keen.client.java.exceptions.ServerException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


/**
 * KeenClientTest
 *
 * @author dkador
 * @since 1.0.0
 */
public class KeenClientTest {

    private static KeenProject TEST_PROJECT;

    /**
     * JSON object mapper that the test infrastructure can use without worrying about any
     * interference with the Keen library's JSON handler.
     */
    private static ObjectMapper JSON_MAPPER;

    private KeenClient client;

    @BeforeClass
    public static void classSetUp() {
        KeenLogging.enableLogging();
        KeenClient client = new TestKeenClient();
        KeenClient.initialize(client);
        TEST_PROJECT = new KeenProject("508339b0897a2c4282000000",
                "80ce00d60d6443118017340c42d1cfaf", "80ce00d60d6443118017340c42d1cfaf");
        JSON_MAPPER = new ObjectMapper();
        JSON_MAPPER.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }

    @Before
    public void setup() {
        client = KeenClient.client();
        client.setBaseUrl(null);
        client.setDebugMode(true);
        client.setDefaultProject(TEST_PROJECT);
    }

    @After
    public void cleanUp() {
        client = null;
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
    public void nullEvent() throws Exception {
        runValidateAndBuildEventTest(null, "foo", "null event",
                "You must specify a non-null, non-empty event.");
    }

    @Test
    public void emptyEvent() throws Exception {
        runValidateAndBuildEventTest(new HashMap<String, Object>(), "foo", "empty event",
                "You must specify a non-null, non-empty event.");
    }

    @Test
    public void eventWithKeenRootProperty() throws Exception {
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("keen", "reserved");
        runValidateAndBuildEventTest(event, "foo", "keen reserved",
                "An event cannot contain a root-level property named 'keen'.");
    }

    @Test
    public void eventWithDotInPropertyName() throws Exception {
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("ab.cd", "whatever");
        runValidateAndBuildEventTest(event, "foo", ". in property name",
                "An event cannot contain a property with the period (.) character in it.");
    }

    @Test
    public void eventWithInitialDollarPropertyName() throws Exception {
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("$a", "whatever");
        runValidateAndBuildEventTest(event, "foo", "$ at start of property name",
                "An event cannot contain a property that starts with the dollar sign ($) " +
                        "character in it.");
    }

    @Test
    public void eventWithTooLongPropertyName() throws Exception {
        Map<String, Object> event = new HashMap<String, Object>();
        String tooLong = TestUtils.getString(257);
        event.put(tooLong, "whatever");
        runValidateAndBuildEventTest(event, "foo", "too long property name",
                "An event cannot contain a property name longer than 256 characters.");
    }

    @Test
    public void eventWithTooLongStringValue() throws Exception {
        Map<String, Object> event = new HashMap<String, Object>();
        String tooLong = TestUtils.getString(10000);
        event.put("long", tooLong);
        runValidateAndBuildEventTest(event, "foo", "too long property value",
                "An event cannot contain a string property value longer than 10,000 characters.");
    }

    @Test
    public void eventWithSelfReferencingProperty() throws Exception {
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("recursion", event);
        runValidateAndBuildEventTest(event, "foo", "self referencing",
                "An event's depth (i.e. layers of nesting) cannot exceed " + KeenConstants.MAX_EVENT_DEPTH);
    }

    @Test
    public void eventWithInvalidPropertyInList() throws Exception {
        Map<String, Object> event = new HashMap<String, Object>();
        List<String> invalidList = new ArrayList<String>();
        String tooLong = TestUtils.getString(10000);
        invalidList.add(tooLong);
        event.put("invalid_list", invalidList);
        runValidateAndBuildEventTest(event, "foo", "invalid value in list",
                "An event cannot contain a string property value longer than 10,000 characters.");
    }

    @Test
    public void validEvent() throws Exception {
        KeenClient client = KeenClient.client();
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("valid key", "valid value");
        Map<String, Object> builtEvent = client.validateAndBuildEvent(client.getDefaultProject(), "foo", event, null);
        assertNotNull(builtEvent);
        assertEquals("valid value", builtEvent.get("valid key"));
        // also make sure the event has been timestamped
        @SuppressWarnings("unchecked")
        Map<String, Object> keenNamespace = (Map<String, Object>) builtEvent.get("keen");
        assertNotNull(keenNamespace);
        assertNotNull(keenNamespace.get("timestamp"));
    }

    @Test
    public void validEventWithTimestamp() throws Exception {
        KeenClient client = KeenClient.client();
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("valid key", "valid value");
        Calendar now = Calendar.getInstance();
        event.put("datetime", now);
        client.validateAndBuildEvent(client.getDefaultProject(), "foo", event, null);
    }

    @Test
    public void validEventWithNestedKeenProperty() throws Exception {
        KeenClient client = KeenClient.client();
        Map<String, Object> event = TestUtils.getSimpleEvent();
        Map<String, Object> nested = new HashMap<String, Object>();
        nested.put("keen", "value");
        event.put("nested", nested);
        client.validateAndBuildEvent(client.getDefaultProject(), "foo", event, null);
    }

    @Test
    public void testAddEventNoWriteKey() throws KeenException, IOException {
        KeenClient client = KeenClient.client();
        client.setDefaultProject(new KeenProject("508339b0897a2c4282000000", null, null));
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("test key", "test value");
        try {
            client.addEvent("foo", event);
            fail("add event without write key should fail");
        } catch (NoWriteKeyException e) {
            assertEquals("You can't send events to Keen IO if you haven't set a write key.",
                    e.getLocalizedMessage());
        }
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
        KeenClient client = KeenClient.client();
        client.setGlobalProperties(globalProperties);
        Map<String, Object> event = TestUtils.getSimpleEvent();
        String eventCollection = String.format("foo%d", Calendar.getInstance().getTimeInMillis());
        Map<String, Object> builtEvent = client.validateAndBuildEvent(client.getDefaultProject(), eventCollection, event, null);
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
        KeenClient client = KeenClient.client();
        client.setGlobalPropertiesEvaluator(evaluator);
        Map<String, Object> event = TestUtils.getSimpleEvent();
        String eventCollection = String.format("foo%d", Calendar.getInstance().getTimeInMillis());
        Map<String, Object> builtEvent = client.validateAndBuildEvent(client.getDefaultProject(), eventCollection, event, null);
        assertEquals(expectedNumProperties + 1, builtEvent.size());
        return builtEvent;
    }

    @Test
    public void testGlobalPropertiesTogether() throws Exception {
        KeenClient client = KeenClient.client();

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
        Map<String, Object> builtEvent = client.validateAndBuildEvent(client.getDefaultProject(), "apples", event, null);

        assertEquals("bar", builtEvent.get("foo"));
        assertEquals(6, builtEvent.get("default property"));
        assertEquals(3, builtEvent.size());
    }

    private void runValidateAndBuildEventTest(Map<String, Object> event, String eventCollection, String msg,
                                              String expectedMessage) {
        KeenClient client = KeenClient.client();
        try {
            client.validateAndBuildEvent(client.getDefaultProject(), eventCollection, event, null);
            fail(msg);
        } catch (KeenException e) {
            assertEquals(expectedMessage, e.getLocalizedMessage());
        }
    }

    private KeenClient getMockedClient(Object data, int statusCode) throws IOException, KeenInitializationException {
        if (data == null) {
            data = buildResponseJson(true, null, null);
        }

        // set up the partial mock
        KeenClient client = KeenClient.client();
        client = spy(client);

        byte[] bytes = JSON_MAPPER.writeValueAsBytes(data);
        ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
        HttpURLConnection connMock = mock(HttpURLConnection.class);
        when(connMock.getResponseCode()).thenReturn(statusCode);
        if (statusCode == 200) {
            when(connMock.getInputStream()).thenReturn(stream);
        } else {
            when(connMock.getErrorStream()).thenReturn(stream);
        }

        doReturn(connMock).when(client).sendQueuedEvents();

        return client;
    }

    private Map<String, Object> buildResponseJson(boolean success, String errorCode, String description) {
        Map<String, Object> result = buildResult(success, errorCode, description);
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        list.add(result);
        Map<String, Object> response = new HashMap<String, Object>();
        response.put("foo", list);
        return response;
    }

    private Map<String, Object> buildResult(boolean success, String errorCode, String description) {
        Map<String, Object> result = new HashMap<String, Object>();
        result.put("success", success);
        if (!success) {
            Map<String, Object> error = new HashMap<String, Object>();
            error.put("name", errorCode);
            error.put("description", description);
            result.put("error", error);
        }
        return result;
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
