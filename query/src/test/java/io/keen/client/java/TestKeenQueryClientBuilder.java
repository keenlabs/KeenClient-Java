package io.keen.client.java;

/**
 * Created by claireyoung on 5/26/15.
 */
public class TestKeenQueryClientBuilder extends KeenQueryClient.QueryBuilder {

    private boolean isNetworkConnected = true;

    public TestKeenQueryClientBuilder(KeenProject project) {
        super(project);
    }

    @Override
    protected KeenJsonHandler getDefaultJsonHandler() {
        return new TestJsonHandler();
    }

    @Override
    protected KeenQueryClient buildInstance() {
        return new KeenQueryClient(this);
    }

}
