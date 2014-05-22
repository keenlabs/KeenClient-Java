package io.keen.client.android;

import android.content.Context;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import io.keen.client.java.KeenClient;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the AndroidKeenClient implementation.
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public class AndroidKeenClientTest {

    private static final File TEST_STORE_ROOT = new File("test_store_root");

    @BeforeClass
    public static void classSetUp() throws IOException {
        KeenLogging.enableLogging();
        FileUtils.forceMkdir(TEST_STORE_ROOT);
    }

    @Before
    public void testSetUp() throws IOException {
        FileUtils.cleanDirectory(TEST_STORE_ROOT);
    }

    @AfterClass
    public static void deleteStoreRoot() throws IOException {
        FileUtils.deleteDirectory(TEST_STORE_ROOT);
    }

    @Test
    public void validContext() {
        AndroidKeenClient.initialize(getMockedContext());
    }

    @Test
    public void nullContext() {
        AndroidKeenClient.initialize(null);
        assertFalse(KeenClient.client().isActive());
    }

    private Context getMockedContext() {
        Context mockContext = mock(Context.class);
        when(mockContext.getApplicationContext()).thenReturn(mockContext);
        when(mockContext.getCacheDir()).thenReturn(TEST_STORE_ROOT);
        return mockContext;
    }

}
