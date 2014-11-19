package io.keen.client.android;

import io.keen.client.java.KeenNetworkStatusHandler;

import android.content.Context;
import android.net.ConnectivityManager;
import android.net.NetworkInfo;

import java.lang.Override;

/**
 * This class implements the KeenNetworkStatusHandler. It uses the available
 * native android functions for checking the network status.
 *
 * @author Simon Murtha Smith
 * @since 2.1.0
 */
public class AndroidNetworkStatusHandler implements KeenNetworkStatusHandler {

    private final Context context;

    public AndroidNetworkStatusHandler(Context context) {
        this.context = context;
    }

    @Override
    public boolean isNetworkConnected() {
        // Check if there is an active network connection
        ConnectivityManager connectivityManager
            = (ConnectivityManager) context.getSystemService(Context.CONNECTIVITY_SERVICE);
        NetworkInfo activeNetworkInfo = connectivityManager.getActiveNetworkInfo();
        return activeNetworkInfo != null && activeNetworkInfo.isConnected();
    }
}
