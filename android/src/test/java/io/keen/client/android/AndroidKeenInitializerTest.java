package io.keen.client.android;

import android.content.Context;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * AndroidKeenInitializerTest
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public class AndroidKeenInitializerTest {

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
        // TODO: Remove this once other tests have been added.
    }

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
