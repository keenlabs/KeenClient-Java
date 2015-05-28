package io.keen.client.java;

/**
 * Created by claireyoung on 5/26/15.
 */
public class TestKeenQueryClientBuilder extends KeenQueryClient.Builder {

    private boolean isNetworkConnected = true;

    public TestKeenQueryClientBuilder() { }

    @Override
    protected KeenJsonHandler getDefaultJsonHandler() {
        return new TestJsonHandler();
    }

    @Override
    protected KeenQueryClient buildInstance() {
        return new KeenQueryClient(this);
    }

}
