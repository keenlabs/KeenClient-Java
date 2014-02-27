package io.keen.client.java;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the FileEventSTore class.
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public class FileEventStoreTest extends EventStoreTestBase {

    private static final File TEST_STORE_ROOT = new File("test_store_root");

    @BeforeClass
    public static void createStoreRoot() throws Exception {
        FileUtils.forceMkdir(TEST_STORE_ROOT);
    }

    @Before
    public void cleanStoreRoot() throws IOException {
        FileUtils.cleanDirectory(TEST_STORE_ROOT);
    }

    @AfterClass
    public static void deleteStoreRoot() throws Exception {
        FileUtils.deleteDirectory(TEST_STORE_ROOT);
    }

    @Override
    protected KeenEventStore buildStore() throws IOException {
        return new FileEventStore(TEST_STORE_ROOT, new TestJsonHandler());
    }

    @Test
    public void existingEventFilesFound() throws Exception {
        writeEventFile("keen/collection1/13412341234.0", "{\"foo\":\"bar\"}");
        writeEventFile("keen/collection1/13412341235.0", "{\"hello\":\"world\"}");
        Map<String, List<Object>> handleMap = store.getHandles();
        assertNotNull(handleMap);
        assertEquals(1, handleMap.size());
        List<Object> handles = handleMap.get("collection1");
        assertNotNull(handles);
        assertEquals(2, handles.size());
        List<Map<String, Object>> events = new ArrayList<Map<String, Object>>();
        for (Object handle : handles) {
            events.add(store.get(handle));
        }
        Map<String, Object> expected = new HashMap<String, Object>();
        expected.put("foo", "bar");
        assertTrue(events.contains(expected));
        expected.clear();
        expected.put("hello", "world");
        assertTrue(events.contains(expected));
    }

    private void writeEventFile(String path, String data) throws IOException {
        File eventFile = new File(TEST_STORE_ROOT, path);
        FileUtils.write(eventFile, data, "UTF-8");
    }
}
