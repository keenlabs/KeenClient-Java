package io.keen.client.android;

import android.content.Context;

import io.keen.client.java.FileEventStore;
import io.keen.client.java.KeenClient;
import io.keen.client.java.KeenEventStore;
import io.keen.client.java.KeenJsonHandler;

/**
 * SHIPBLOCK: Fix comments.
 * Implementation of a {@link io.keen.client.java.KeenClient} on the Android platform.
 * <p/>
 * This client uses the built-in Android JSON libraries for reading/writing JSON in order to
 * minimize library size.
 * <p/>
 * To cache events in between batch uploads, this client uses a file-based event store with its
 * root in the application's cache directory.
 * <p/>
 * This client uses a fixed thread pool to run asynchronous requests.
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

}
