package io.keen.client.android;

import android.content.Context;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

import static org.mockito.Mockito.*;

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

    @Before
    public void testSetUp() {
        TestUtils.deleteRecursively(getKeenClientCacheDir());
    }

    @Test
    public void dummyTest() {
        // Do nothing.
    }

    /*
    @Test
    public void testKeenClientConstructor() {
        Context context = getMockedContext();

        runKeenClientConstructorTest(null, null, null, null, true, "null context",
                "Android Context cannot be null.");
        runKeenClientConstructorTest(context, null, null, null, true, "null project id",
                "Invalid project ID specified: null");
        runKeenClientConstructorTest(context, "", null, null, true, "empty project id",
                "Invalid project ID specified: ");
        runKeenClientConstructorTest(context, "abc", null, null, false, "everything is good",
                null);
        runKeenClientConstructorTest(context, "abc", "def", "ghi", false, "keys",
                null);
    }

    @Test
    public void testUploadNoEvents() {
        // shouldn't cause any uncatchable (async task) exception
        // needed to move above other upload tests to get this to repro the bug?
        try {
            KeenClient client = getMockedClient(null, 200);
            // don't add any events
            client.upload(null);
        } catch (Exception e){
            fail("Calling upload with no events caused an exception! \n" + e.toString());
        }
    }

    @Test
    public void testUploadEventAndThenUploadNoEvents()  {
        // internally catches and logs ERROR: There was a JsonMappingException while sending {foo=[]}
        try {
            KeenClient client = getMockedClient(null, 200);
            Map<String, Object> event = TestUtils.getSimpleEvent();
            client.addEvent("foo", event);
            // upload some events
            client.upload(null);
            // don't add any more events
            client.upload(null);
        } catch (Exception e){
            fail("Calling upload twice, second time with no events caused an exception! \n" + e.toString());
        }
    }

    @Test
    public void testUploadEventAndThenUploadSameEventAgain()  {
        // internally catches and logs ERROR: There was a JsonMappingException while sending
        // {foo=[{a=b, keen={timestamp=2013-12-18T19:11:50.311+0000}}]}
        try {
            KeenClient client = getMockedClient(null, 200);
            Map<String, Object> event = TestUtils.getSimpleEvent();
            client.addEvent("foo", event);
            // upload some events
            client.upload(null);
            // add a different collection event
            client = getMockedClient(null, 200);
            client.addEvent("foo", event);
            //client.addEvent("bar", event);
            client.upload(null);
        } catch (Exception e){
            fail("Calling upload twice, second time with same event caused an exception! \n" + e.toString());
        }
    }

    @Test
    public void testUploadEventAndThenUploadNewEventButNotOld()  {
        // internally catches and logs ERROR: There was a JsonMappingException while sending
        // {foo=[], bar=[{a=b, keen={timestamp=2013-12-18T19:11:50.316+0000}}]}
        try {
            Object individualResult = buildResult(true, null, null);
            List<Object> list = new ArrayList<Object>();
            list.add(individualResult);
            Map<String, Object> resultFoo = new HashMap<String, Object>();
            String eventCollectionFoo = "foo";
            resultFoo.put(eventCollectionFoo, list);

            Map<String, Object> resultBar = new HashMap<String, Object>();
            String eventCollectionBar = "bar";
            resultBar.put(eventCollectionBar, list);


            KeenClient client = getMockedClient(resultFoo, 200);
            Map<String, Object> event = TestUtils.getSimpleEvent();
            client.addEvent("foo", event);
            // upload some events
            client.upload(null);
            // add a different collection event
            client = getMockedClient(resultBar, 200);
            client.addEvent("bar", event);
            client.upload(null);
        } catch (Exception e){
            fail("Calling upload twice, second time with different event caused an exception! \n" + e.toString());
        }
    }

    @Test
    public void testUploadEventAndThenUploadNewEventAndOld()  {
        // internally catches and logs ERROR: There was a JsonMappingException while sending
        // {foo=[{a=b, keen={timestamp=2013-12-18T19:11:50.321+0000}}], bar=[{a=b, keen={timestamp=2013-12-18T19:11:50.323+0000}}]}
        try {
            KeenClient client = getMockedClient(null, 200);
            Map<String, Object> event = TestUtils.getSimpleEvent();
            client.addEvent("foo", event);
            // upload some events
            client.upload(null);
            // add a different collection event
            client = getMockedClient(null, 200);
            client.addEvent("foo", event);
            client.addEvent("bar", event);
            client.upload(null);
        } catch (Exception e){
            fail("Calling upload twice, second time with different event caused an exception! \n" + e.toString());
        }
    }

    @Test
    public void testAddEvent() throws KeenException, IOException {
        runAddEventTestFail(null, "foo", "null event",
                "You must specify a non-null, non-empty event.");

        runAddEventTestFail(new HashMap<String, Object>(), "foo", "empty event",
                "You must specify a non-null, non-empty event.");

        Map<String, Object> event = new HashMap<String, Object>();
        event.put("keen", "reserved");
        runAddEventTestFail(event, "foo", "keen reserved",
                "An event cannot contain a root-level property named 'keen'.");

        event.remove("keen");
        event.put("ab.cd", "whatever");
        runAddEventTestFail(event, "foo", ". in property name",
                "An event cannot contain a property with the period (.) character in it.");

        event.remove("ab.cd");
        event.put("$a", "whatever");
        runAddEventTestFail(event, "foo", "$ at start of property name",
                "An event cannot contain a property that starts with the dollar sign ($) character in it.");

        event.remove("$a");
        String tooLong = TestUtils.getString(257);
        event.put(tooLong, "whatever");
        runAddEventTestFail(event, "foo", "too long property name",
                "An event cannot contain a property name longer than 256 characters.");

        event.remove(tooLong);
        tooLong = TestUtils.getString(10000);
        event.put("long", tooLong);
        runAddEventTestFail(event, "foo", "too long property value",
                "An event cannot contain a string property value longer than 10,000 characters.");

        // now do a basic add
        event.remove("long");
        event.put("valid key", "valid value");
        KeenClient client = getClient();
        client.addEvent("foo", event);
        // make sure the event's there
        Map<String, Object> storedEvent = getFirstEventForCollection(client, "foo");
        assertNotNull(storedEvent);
        assertEquals("valid value", storedEvent.get("valid key"));
        // also make sure the event has been timestamped
        @SuppressWarnings("unchecked")
        Map<String, Object> keenNamespace = (Map<String, Object>) storedEvent.get("keen");
        assertNotNull(keenNamespace);
        assertNotNull(keenNamespace.get("timestamp"));

        // an event with a Calendar should work
        Calendar now = Calendar.getInstance();
        event.put("datetime", now);
        client.addEvent("foo", event);
        File[] files = client.getFilesInDir(client.getEventDirectoryForEventCollection("foo"));
        assertEquals(2, files.length);

        // an event with a nested property called "keen" should work
        event = TestUtils.getSimpleEvent();
        Map<String, Object> nested = new HashMap<String, Object>();
        nested.put("keen", "value");
        event.put("nested", nested);
        client.addEvent("foo", event);
        files = client.getFilesInDir(client.getEventDirectoryForEventCollection("foo"));
        assertEquals(3, files.length);
    }

    @Test
    public void testUploadSuccess() throws KeenException, IOException {
        // this is the only test that does a full round-trip to the real API. the others mock out the connection.
        KeenClient client = getClient();
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("test key", "test value");
        client.addEvent("foo", event);
        client.upload(null);
        // make sure file was deleted
        assertNull(getFirstEventForCollection(client, "foo"));
    }

    @Test
    public void testUploadFailedServerDown() throws Exception {
        KeenClient client = getMockedClient("", 500);
        addSimpleEventAndUpload(client);
        // make sure the file wasn't deleted locally
        assertNotNull(getFirstEventForCollection(client, "foo"));
    }

    @Test
    public void testUploadFailedServerDownNonJsonResponse() throws Exception {
        KeenClient client = getMockedClient("bad data", 500);
        addSimpleEventAndUpload(client);
        // make sure the file wasn't deleted locally
        assertNotNull(getFirstEventForCollection(client, "foo"));
    }

    @Test
    public void testUploadFailedBadRequest() throws Exception {
        Object response = buildResponseJson(false, "InvalidCollectionNameError", "anything");
        KeenClient client = getMockedClient(response, 200);
        addSimpleEventAndUpload(client);
        // make sure the file was deleted locally
        assertNull(getFirstEventForCollection(client, "foo"));
    }

    @Test
    public void testUploadFailedBadRequestUnknownError() throws Exception {
        KeenClient client = getMockedClient("doesn't matter", 400);
        addSimpleEventAndUpload(client);
        // make sure the file wasn't deleted locally
        assertNotNull(getFirstEventForCollection(client, "foo"));
    }

    @Test
    public void testUploadMultipleEventsSameCollectionSuccess() throws Exception {
        Object individualResult = buildResult(true, null, null);
        List<Object> list = new ArrayList<Object>();
        // add it twice
        list.add(individualResult);
        list.add(individualResult);
        Map<String, Object> result = new HashMap<String, Object>();
        String eventCollection = "foo";
        result.put(eventCollection, list);

        Map<String, Object> event = TestUtils.getSimpleEvent();

        KeenClient client = getMockedClient(result, 200);
        client.addEvent(eventCollection, event);
        client.addEvent(eventCollection, event);

        client.upload(null);

        assertNull(getFirstEventForCollection(client, eventCollection));
    }

    @Test
    public void testUploadMultipleEventsDifferentCollectionsSuccess() throws Exception {
        Object individualResult = buildResult(true, null, null);
        List<Object> list1 = new ArrayList<Object>();
        List<Object> list2 = new ArrayList<Object>();
        list1.add(individualResult);
        list2.add(individualResult);
        Map<String, Object> result = new HashMap<String, Object>();
        result.put("foo", list1);
        result.put("bar", list2);

        Map<String, Object> event = TestUtils.getSimpleEvent();

        KeenClient client = getMockedClient(result, 200);
        client.addEvent("foo", event);
        client.addEvent("bar", event);

        client.upload(null);

        assertNull(getFirstEventForCollection(client, "foo"));
        assertNull(getFirstEventForCollection(client, "bar"));
    }

    @Test
    public void testUploadMultipleEventsSameCollectionOneFails() throws Exception {
        Object result1 = buildResult(true, null, null);
        Object result2 = buildResult(false, "InvalidCollectionNameError", "anything");
        List<Object> list1 = new ArrayList<Object>();
        list1.add(result1);
        list1.add(result2);
        Map<String, Object> result = new HashMap<String, Object>();
        result.put("foo", list1);

        Map<String, Object> event = TestUtils.getSimpleEvent();

        KeenClient client = getMockedClient(result, 200);
        client.addEvent("foo", event);
        client.addEvent("foo", event);

        client.upload(null);

        assertNull(getFirstEventForCollection(client, "foo"));
    }

    @Test
    public void testUploadMultipleEventsDifferentCollectionsOneFails() throws Exception {
        Object result1 = buildResult(true, null, null);
        Object result2 = buildResult(false, "InvalidCollectionNameError", "anything");
        List<Object> list1 = new ArrayList<Object>();
        List<Object> list2 = new ArrayList<Object>();
        list1.add(result1);
        list2.add(result2);
        Map<String, Object> result = new HashMap<String, Object>();
        result.put("foo", list1);
        result.put("bar", list2);

        Map<String, Object> event = TestUtils.getSimpleEvent();

        KeenClient client = getMockedClient(result, 200);
        client.addEvent("foo", event);
        client.addEvent("bar", event);

        client.upload(null);

        assertNull(getFirstEventForCollection(client, "foo"));
        assertNull(getFirstEventForCollection(client, "bar"));
    }

    @Test
    public void testUploadMultipleEventsDifferentCollectionsOneFailsForServerReason() throws Exception {
        Object result1 = buildResult(true, null, null);
        Object result2 = buildResult(false, "barf", "anything");
        List<Object> list1 = new ArrayList<Object>();
        List<Object> list2 = new ArrayList<Object>();
        list1.add(result1);
        list2.add(result2);
        Map<String, Object> result = new HashMap<String, Object>();
        result.put("foo", list1);
        result.put("bar", list2);

        Map<String, Object> event = TestUtils.getSimpleEvent();

        KeenClient client = getMockedClient(result, 200);
        client.addEvent("foo", event);
        client.addEvent("bar", event);

        client.upload(null);

        assertNull(getFirstEventForCollection(client, "foo"));
        assertNotNull(getFirstEventForCollection(client, "bar"));
    }

    @Test
    public void testTooManyEventsCached() throws Exception {
        KeenClient client = getClient();
        Map<String, Object> event = TestUtils.getSimpleEvent();
        // create 5 events
        for (int i = 0; i < 5; i++) {
            client.addEvent("foo", event);
        }

        // should be 5 events now
        File[] files = client.getFilesForEventCollection("foo");
        assertEquals(5, files.length);
        // now do 1 more, should age out 2 old ones
        client.addEvent("foo", event);
        // so now there should be 4 left (5 - 2 + 1)
        assertEquals(4, client.getFilesForEventCollection("foo").length);
    }

    @Test
    public void testGetKeenCacheDirectory() throws Exception {
        File dir = new File(getMockedContext().getCacheDir(), "keen");
        if (dir.exists()) {
            assert dir.delete();
        }

        KeenClient client = getClient();
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("a", "apple");
        client.addEvent("foo", event);
    }

    private void addSimpleEventAndUpload(KeenClient mockedClient) throws KeenException {
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("a", "apple");
        mockedClient.addEvent("foo", event);
        mockedClient.upload(null);
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

    private Map<String, Object> buildResponseJson(boolean success, String errorCode, String description) {
        Map<String, Object> result = buildResult(success, errorCode, description);
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        list.add(result);
        Map<String, Object> response = new HashMap<String, Object>();
        response.put("foo", list);
        return response;
    }

    private KeenClient getMockedClient(Object data, int statusCode) throws IOException {
        if (data == null) {
            data = buildResponseJson(true, null, null);
        }

        // set up the partial mock
        KeenClient client = getClient();
        client = spy(client);

        byte[] bytes = KeenClient.MAPPER.writeValueAsBytes(data);
        ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
        HttpURLConnection connMock = mock(HttpURLConnection.class);
        when(connMock.getResponseCode()).thenReturn(statusCode);
        if (statusCode == 200) {
            when(connMock.getInputStream()).thenReturn(stream);
        } else {
            when(connMock.getErrorStream()).thenReturn(stream);
        }

        doReturn(connMock).when(client).sendEvents(Matchers.<Map<String, List<Map<String, Object>>>>any());

        return client;
    }

    private Map<String, Object> getFirstEventForCollection(KeenClient client,
                                                           String eventCollection) throws IOException {
        File dir = client.getEventDirectoryForEventCollection(eventCollection);
        File[] files = client.getFilesInDir(dir);
        if (files.length == 0) {
            return null;
        } else {
            return KeenClient.MAPPER.readValue(files[0], new TypeReference<Map<String, Object>>() {
            });
        }
    }

    private void runAddEventTestFail(Map<String, Object> event, String eventCollection, String msg,
                                     String expectedMessage) {
        KeenClient client = getClient();
        try {
            client.addEvent(eventCollection, event);
            fail(msg);
        } catch (KeenException e) {
            assertEquals(expectedMessage, e.getLocalizedMessage());
        }
    }

    private void doClientAssertions(Context expectedContext, String expectedProjectId,
                                    String expectedWriteKey, String expectedReadKey, KeenClient client) {
        assertEquals(expectedContext, client.getContext());
        assertEquals(expectedProjectId, client.getProjectId());
        assertEquals(expectedWriteKey, client.getWriteKey());
        assertEquals(expectedReadKey, client.getReadKey());
    }

    private KeenClient getClient() {
        return getClient("508339b0897a2c4282000000", "80ce00d60d6443118017340c42d1cfaf",
                "80ce00d60d6443118017340c42d1cfaf");
    }

    private KeenClient getClient(String projectId, String writeKey, String readKey) {
        KeenClient client = new KeenClient(getMockedContext(), projectId, writeKey, readKey);
        client.setIsRunningTests(true);
        return client;
    }

    */

    private Context getMockedContext() {
        Context mockContext = mock(Context.class);
        when(mockContext.getCacheDir()).thenReturn(getCacheDir());
        return mockContext;
    }

    private static File getCacheDir() {
        return new File("/tmp");
    }

    private static File getKeenClientCacheDir() {
        return new File(getCacheDir(), "keen");
    }

}
