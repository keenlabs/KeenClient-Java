package io.keen.client.java;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.keen.client.java.exceptions.KeenException;
import io.keen.client.java.exceptions.NoWriteKeyException;
import io.keen.client.java.exceptions.ServerException;
import io.keen.client.java.http.HttpHandler;
import io.keen.client.java.http.Request;
import io.keen.client.java.http.Response;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * KeenClientTest
 *
 * @author dkador
 * @since 1.0.0
 */
public class KeenClientTest {

    private static KeenProject TEST_PROJECT;

    private static List<Map<String, Object>> TEST_EVENTS;

    private static final String TEST_COLLECTION = "test_collection";
    private static final String TEST_COLLECTION_2 = "test_collection_2";

    private static final String POST_EVENT_SUCCESS = "{\"created\": true}";

    /**
     * JSON object mapper that the test infrastructure can use without worrying about any
     * interference with the Keen library's JSON handler.
     */
    private static ObjectMapper JSON_MAPPER;

    private KeenClient client;
    private HttpHandler mockHttpHandler;
    private TestKeenClientBuilder builder;

    @BeforeClass
    public static void classSetUp() {
        KeenLogging.enableLogging();
        TEST_PROJECT = new KeenProject("<project ID>", "<write key>", "<read key");
        TEST_EVENTS = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < 10; i++) {
            Map<String, Object> event = new HashMap<String, Object>();
            event.put("test-key", "test-value-" + i);
            TEST_EVENTS.add(event);
        }
        JSON_MAPPER = new ObjectMapper();
        JSON_MAPPER.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }

    @Before
    public void setup() throws IOException {
        // Set up a mock HTTP handler.
        mockHttpHandler = mock(HttpHandler.class);
        setMockResponse(500, "Unexpected HTTP request");

        // Build the client.
        builder = new TestKeenClientBuilder();
        client = builder
                .withHttpHandler(mockHttpHandler)
                .build();

        client.setBaseUrl(null);
        client.setDebugMode(true);
        client.setDefaultProject(TEST_PROJECT);

        // Clear the RAM event store.
        ((RamEventStore) client.getEventStore()).clear();
    }

    @After
    public void cleanUp() {
        client = null;
    }

    @Test
    public void initializeWithEnvironmentVariables() throws Exception {
        // Construct a new test client and make sure it doesn't have a default project.
        KeenClient testClient = new TestKeenClientBuilder().build();
        assertNull(testClient.getDefaultProject());

        // Mock an environment with a project.
        Environment mockEnv = mock(Environment.class);
        when(mockEnv.getKeenProjectId()).thenReturn("<project ID>");

        // Make sure a new test client using the mock environment has the expected default project.
        testClient = new TestKeenClientBuilder(mockEnv).build();
        KeenProject defaultProject = testClient.getDefaultProject();
        assertNotNull(defaultProject);
        assertEquals("<project ID>", defaultProject.getProjectId());
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
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("valid key", "valid value");
        Calendar now = Calendar.getInstance();
        event.put("datetime", now);
        client.validateAndBuildEvent(client.getDefaultProject(), "foo", event, null);
    }

    @Test
    public void validEventWithKeenPropertiesWithoutTimestamp() throws Exception {
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("valid key", "valid value");
        Map<String, Object> keenProperties = new HashMap<String, Object>();
        keenProperties.put("keen key", "keen value");
        Map<String, Object> result = client.validateAndBuildEvent(client.getDefaultProject(), "foo", event, keenProperties);
        @SuppressWarnings("unchecked")
        Map<String, Object> keenPropResult = (Map<String, Object>)result.get("keen");
        assertNotNull(keenPropResult.get("timestamp"));
        assertNull(keenProperties.get("timestamp"));
        assertEquals(keenProperties.get("keen key"), "keen value");
        assertEquals(keenPropResult.get("keen key"), "keen value");
        assertEquals(keenProperties.get("keen key"), keenPropResult.get("keen key"));
    }

    @Test
    public void validEventWithNestedKeenProperty() throws Exception {
        Map<String, Object> event = TestUtils.getSimpleEvent();
        Map<String, Object> nested = new HashMap<String, Object>();
        nested.put("keen", "value");
        event.put("nested", nested);
        client.validateAndBuildEvent(client.getDefaultProject(), "foo", event, null);
    }

    @Test
    public void testAddEventNoWriteKey() throws KeenException, IOException {
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
        setMockResponse(201, POST_EVENT_SUCCESS);
        client.addEvent(TEST_COLLECTION, TEST_EVENTS.get(0));
        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        verify(mockHttpHandler).execute(capturedRequest.capture());
        assertThat(capturedRequest.getValue().url.toString(), startsWith("https://api.keen.io"));
    }

    @Test
    public void testAddEventNonSSL() throws Exception {
        setMockResponse(201, POST_EVENT_SUCCESS);
        client.setBaseUrl("http://api.keen.io");
        client.addEvent(TEST_COLLECTION, TEST_EVENTS.get(0));
        ArgumentCaptor<Request> capturedRequest = ArgumentCaptor.forClass(Request.class);
        verify(mockHttpHandler).execute(capturedRequest.capture());
        assertThat(capturedRequest.getValue().url.toString(), startsWith("http://api.keen.io"));
    }

    @Test(expected = ServerException.class)
    public void testAddEventServerFailure() throws Exception {
        setMockResponse(500, "Injected server error");
        client.addEvent(TEST_COLLECTION, TEST_EVENTS.get(0));
    }

    @Test
    public void testAddEventWithCallback() throws Exception {
        setMockResponse(201, POST_EVENT_SUCCESS);
        final CountDownLatch latch = new CountDownLatch(1);
        client.addEvent(null, TEST_COLLECTION, TEST_EVENTS.get(0), null, new LatchKeenCallback(latch));
        latch.await(2, TimeUnit.SECONDS);
    }

    @Test
    public void testSendQueuedEvents() throws Exception {
        // Mock the response from the server.
        Map<String, Integer> expectedResponse = new HashMap<String, Integer>();
        expectedResponse.put(TEST_COLLECTION, 3);
        setMockResponse(200, getPostEventsResponse(buildSuccessMap(expectedResponse)));

        // Queue some events.
        client.queueEvent(TEST_COLLECTION, TEST_EVENTS.get(0));
        client.queueEvent(TEST_COLLECTION, TEST_EVENTS.get(1));
        client.queueEvent(TEST_COLLECTION, TEST_EVENTS.get(2));

        // Check that the expected number of events are in the store.
        RamEventStore store = (RamEventStore) client.getEventStore();
        Map<String, List<Object>> handleMap = store.getHandles(TEST_PROJECT.getProjectId());
        assertEquals(1, handleMap.size());
        assertEquals(3, handleMap.get(TEST_COLLECTION).size());

        // Send the queued events.
        client.sendQueuedEvents();

        // Validate that the store is now empty.
        handleMap = store.getHandles(TEST_PROJECT.getProjectId());
        assertEquals(0, handleMap.size());

        // Try sending events again; this should be a no-op.
        setMockResponse(200, "{}");
        client.sendQueuedEvents();

        // Validate that the store is still empty.
        handleMap = store.getHandles(TEST_PROJECT.getProjectId());
        assertEquals(0, handleMap.size());
    }

    @Test
    public void testSendQueuedEventsWithSingleFailure() throws Exception {
        // Queue some events.
        client.queueEvent(TEST_COLLECTION, TEST_EVENTS.get(0));
        client.queueEvent(TEST_COLLECTION, TEST_EVENTS.get(1));
        client.queueEvent(TEST_COLLECTION, TEST_EVENTS.get(2));

        // Check that the expected number of events are in the store.
        RamEventStore store = (RamEventStore) client.getEventStore();
        Map<String, List<Object>> handleMap = store.getHandles(TEST_PROJECT.getProjectId());
        assertEquals(1, handleMap.size());
        assertEquals(3, handleMap.get(TEST_COLLECTION).size());

        // Mock a response containing an error.
        Map<String, Integer> expectedResponse = new HashMap<String, Integer>();
        expectedResponse.put(TEST_COLLECTION, 3);
        Map<String, Object> responseMap = buildSuccessMap(expectedResponse);
        replaceSuccessWithFailure(responseMap, TEST_COLLECTION, 2, "TestInjectedError",
                "This is an error injected by the unit test code");
        setMockResponse(200, getPostEventsResponse(responseMap));

        // Send the events.
        client.sendQueuedEvents();

        // Validate that the store still contains the failed event, but not the other events.
        handleMap = store.getHandles(TEST_PROJECT.getProjectId());
        assertEquals(1, handleMap.size());
        List<Object> handles = handleMap.get(TEST_COLLECTION);
        assertEquals(1, handles.size());
        Object handle = handles.get(0);
        assertThat(store.get(handle), containsString("test-value-2"));
    }

    @Test
    public void testSendQueuedEventsWithServerFailure() throws Exception {
        // Queue some events.
        client.queueEvent(TEST_COLLECTION, TEST_EVENTS.get(0));
        client.queueEvent(TEST_COLLECTION, TEST_EVENTS.get(1));
        client.queueEvent(TEST_COLLECTION, TEST_EVENTS.get(2));

        // Mock a server failure.
        setMockResponse(500, "Injected server failure");

        // Send the events.
        try {
            client.sendQueuedEvents();
        } catch (ServerException e) {
            // This exception is expected; continue.
        }

        // Validate that the store still contains all the events.
        RamEventStore store = (RamEventStore) client.getEventStore();
        Map<String, List<Object>> handleMap = store.getHandles(TEST_PROJECT.getProjectId());
        assertEquals(1, handleMap.size());
        List<Object> handles = handleMap.get(TEST_COLLECTION);
        assertEquals(3, handles.size());
    }

    @Test
    public void testSendQueuedEventsConcurrentProjects() throws Exception {
        // Queue some events in each of two separate projects
        KeenProject otherProject = new KeenProject("<other project>", "<write>", "<read>");
        client.queueEvent(TEST_PROJECT, TEST_COLLECTION, TEST_EVENTS.get(0), null, null);
        client.queueEvent(TEST_PROJECT, TEST_COLLECTION_2, TEST_EVENTS.get(1), null, null);
        client.queueEvent(otherProject, TEST_COLLECTION, TEST_EVENTS.get(2), null, null);
        client.queueEvent(otherProject, TEST_COLLECTION, TEST_EVENTS.get(3), null, null);

        // Check that the expected number of events are in the store, in the expected collections
        RamEventStore store = (RamEventStore) client.getEventStore();
        Map<String, List<Object>> handleMap = store.getHandles(TEST_PROJECT.getProjectId());
        assertEquals(2, handleMap.size());
        assertEquals(1, handleMap.get(TEST_COLLECTION).size());
        assertEquals(1, handleMap.get(TEST_COLLECTION_2).size());
        handleMap = store.getHandles(otherProject.getProjectId());
        assertEquals(1, handleMap.size());
        assertEquals(2, handleMap.get(TEST_COLLECTION).size());
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
        client.setGlobalPropertiesEvaluator(evaluator);
        Map<String, Object> event = TestUtils.getSimpleEvent();
        String eventCollection = String.format("foo%d", Calendar.getInstance().getTimeInMillis());
        Map<String, Object> builtEvent = client.validateAndBuildEvent(client.getDefaultProject(), eventCollection, event, null);
        assertEquals(expectedNumProperties + 1, builtEvent.size());
        return builtEvent;
    }

    @Test
    public void testGlobalPropertiesTogether() throws Exception {
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
        try {
            client.validateAndBuildEvent(client.getDefaultProject(), eventCollection, event, null);
            fail(msg);
        } catch (KeenException e) {
            assertEquals(expectedMessage, e.getLocalizedMessage());
        }
    }

    private void setMockResponse(int statusCode, String body) throws IOException {
        Response response = new Response(statusCode, body);
        when(mockHttpHandler.execute(any(Request.class))).thenReturn(response);
    }

    private Map<String, Object> buildSuccessMap(Map<String, Integer> postedEvents) throws IOException {
        // Build a map that will represent the response.
        Map<String, Object> response = new HashMap<String, Object>();

        // Create a single map for a successfully posted event; this can be reused for each event.
        final Map<String, Boolean> success = new HashMap<String, Boolean>();
        success.put("success", true);

        // Build the response map by creating a list of the appropriate number of successes for
        // each event collection.
        for (Map.Entry<String, Integer> entry : postedEvents.entrySet()) {
            List<Map<String, Boolean>> list = new ArrayList<Map<String, Boolean>>();
            for (int i = 0; i < entry.getValue(); i++) {
                list.add(success);
            }
            response.put(entry.getKey(), list);
        }

        // Return the success map.
        return response;
    }

    @SuppressWarnings("unchecked")
    private void replaceSuccessWithFailure(Map<String, Object> response, String collection,
                                           int index, String errorName, String errorDescription) {

        // Build the failure map.
        Map<String, Object> failure = new HashMap<String, Object>();
        failure.put("success", Boolean.FALSE);
        Map<String, String> reason = new HashMap<String, String>();
        reason.put("name", errorName);
        reason.put("description", errorDescription);
        failure.put("error", reason);

        // Replace the element at the appropriate index with the failure.
        List<Object> eventStatuses = (List<Object>) response.get(collection);
        eventStatuses.set(index, failure);
    }

    private String getPostEventsResponse(Map<String, Object> postedEvents) throws IOException {
        return JSON_MAPPER.writeValueAsString(postedEvents);
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

    @Test
    public void testProxy() throws Exception {
        KeenClient testClient = new TestKeenClientBuilder().build();

        testClient.setProxy("1.2.3.4", 1234);
        assertNotNull(testClient.getProxy());
        assertEquals("/1.2.3.4:1234", testClient.getProxy().address().toString());

        testClient.setProxy(null);
        assertNull(testClient.getProxy());
    }

    @Test
    public void testNotConnected() throws Exception {
        RamEventStore store = (RamEventStore) client.getEventStore();

        // Queue some events.
        client.queueEvent(TEST_COLLECTION, TEST_EVENTS.get(0));
        client.queueEvent(TEST_COLLECTION, TEST_EVENTS.get(1));
        client.queueEvent(TEST_COLLECTION, TEST_EVENTS.get(2));

        // Mock a server success. This shouldn't get accessed until
        // builder.isNetworkConnected() is true. It is here because
        // if the isNetworkConnected function doesn't work properly, we should
        // get a 200 and clear all events, which would cause the first half of
        // this test to fail.
        Map<String, Integer> expectedResponse = new HashMap<String, Integer>();
        expectedResponse.put(TEST_COLLECTION, 3);
        setMockResponse(200, getPostEventsResponse(buildSuccessMap(expectedResponse)));

        Map<String, List<Object>> handleMap = store.getHandles(TEST_PROJECT.getProjectId());
        assertEquals(1, handleMap.size());
        List<Object> handles = handleMap.get(TEST_COLLECTION);
        assertEquals(3, handles.size());

        // ensure that the events remain if there no network.
        builder.setNetworkConnected(false);

        // Attempt to send the events.
        client.sendQueuedEvents();

        // Validate that the store still contains all the events.
        handleMap = store.getHandles(TEST_PROJECT.getProjectId());
        assertEquals(1, handleMap.size());
        handles = handleMap.get(TEST_COLLECTION);
        assertEquals(3, handles.size());


        // now, ensure that the events get cleared if there is network.
        builder.setNetworkConnected(true);

        // Actually send the events.
        client.sendQueuedEvents();

        // Validate that the store no longer contains the events.
        handleMap = store.getHandles(TEST_PROJECT.getProjectId());
        assertEquals(0, handleMap.size());
    }

}
