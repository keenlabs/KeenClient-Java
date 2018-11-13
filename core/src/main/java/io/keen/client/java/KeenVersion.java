package io.keen.client.java;

public class KeenVersion {

    private static final String SDK_VERSION = "5.4.0";

    private KeenVersion() {
    }

    public static String getSdkVersion() {
        return SDK_VERSION;
    }

}
