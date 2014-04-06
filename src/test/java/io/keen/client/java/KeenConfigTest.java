package io.keen.client.java;

import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class KeenConfigTest {

    @After
    public void tearDown() throws Exception {
        KeenConfig.clearProxy();
    }

    @Test
    public void proxyStartOffEmpty() throws Exception {
        assertNull(KeenConfig.getProxy());
    }

    @Test
    public void canConfigureProxyObject() throws Exception {

        KeenConfig.setProxy("foo", 8080);

        assertEquals("foo:8080", KeenConfig.getProxy().address().toString());
    }

    @Test
    public void canClearProxy() throws Exception {
        KeenConfig.setProxy("foo", 8080);

        KeenConfig.clearProxy();

        assertNull(KeenConfig.getProxy());
    }
}
