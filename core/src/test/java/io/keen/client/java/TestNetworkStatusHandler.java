package io.keen.client.java;

public class TestNetworkStatusHandler implements KeenNetworkStatusHandler {
    private boolean isNetworkConnected;

    public TestNetworkStatusHandler(boolean isNetworkConnected) {
        this.isNetworkConnected = isNetworkConnected;
    }

    public void setNetworkConnected(boolean isNetworkConnected) {
        this.isNetworkConnected = isNetworkConnected;
    }

    public TestNetworkStatusHandler withNetworkConnected(boolean isNetworkConnected) {
        setNetworkConnected(isNetworkConnected);
        return this;
    }

    public boolean isNetworkConnected() {
        return isNetworkConnected;
    }
}
