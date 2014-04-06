package io.keen.client.java;

import org.junit.Test;

import java.io.IOException;
import java.net.*;

import static org.junit.Assert.*;

public class KeenHttpRequestRunnableTest {

    private Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress("/", 8080));

    @Test
    public void willUseProxyIfConfigured() throws Exception {

        KeenHttpRequestRunnable request = new KeenHttpRequestRunnable(null,null, null, null, proxy);

        MockHandler handler = new MockHandler();

        URL url = new URL("foo", "bar", 99, "/foobar", handler);

        request.openConnection(url);

        assertTrue(handler.calledWithProxy);
        assertEquals(proxy, handler.openedProxy);
    }

    @Test
    public void noProxyConfiguredWillNotUseAProxy() throws Exception {

        KeenHttpRequestRunnable request = new KeenHttpRequestRunnable(null,null, null, null, null);

        MockHandler handler = new MockHandler();

        URL url = new URL("foo", "bar", 99, "/foobar", handler);

        request.openConnection(url);

        assertTrue(handler.calledWithoutProxy);
        assertNull(handler.openedProxy);
    }

    class MockHandler extends URLStreamHandler {
        boolean calledWithProxy;
        boolean calledWithoutProxy;
        Proxy openedProxy;

        @Override
        protected URLConnection openConnection(URL u) throws IOException {
            calledWithoutProxy = true;
            return null;
        }

        @Override
        protected URLConnection openConnection(URL u, Proxy p) throws IOException {
            calledWithProxy = true;
            openedProxy = p;
            return null;
        }
    };
}
