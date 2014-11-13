package io.keen.client.android;

import android.content.Context;
import android.net.ConnectivityManager;
import android.net.NetworkInfo;

import io.keen.client.java.FileEventStore;
import io.keen.client.java.KeenClient;
import io.keen.client.java.KeenEventStore;
import io.keen.client.java.KeenJsonHandler;

/**
 * {@link io.keen.client.java.KeenClient.Builder} with defaults suited for use on the Android
 * platform.
 * <p/>
 * This client uses the built-in Android JSON libraries for reading/writing JSON in order to
 * minimize library size. For applications which already include a more robust JSON library such
 * as Jackson or GSON, configure the builder to use an appropriate {@link KeenJsonHandler} via
 * the {@link #withJsonHandler(KeenJsonHandler)} method.
 * <p/>
 * To cache events in between batch uploads, this client uses a file-based event store with its
 * root in the application's cache directory. It is important to use a file-based (or
 * otherwise persistent, i.e. non-RAM) event store because the application process could be
 * destroyed without notice.
 * <p/>
 * Other defaults are those provided by the parent {@link io.keen.client.java.KeenClient.Builder}
 * implementation.
 * <p/>
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public class AndroidKeenClientBuilder extends KeenClient.Builder {

    private final Context context;

    public AndroidKeenClientBuilder(Context context) {
        this.context = context;
    }

    @Override
    protected KeenJsonHandler getDefaultJsonHandler() {
        return new AndroidJsonHandler();
    }

    @Override
    protected KeenEventStore getDefaultEventStore() throws Exception {
        return new FileEventStore(context.getCacheDir());
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
