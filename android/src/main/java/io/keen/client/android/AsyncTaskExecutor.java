package io.keen.client.android;

import android.os.AsyncTask;

import java.util.concurrent.Executor;

/**
 * Implementation of the {@link java.util.concurrent.Executor} interface which uses an
 * {@link android.os.AsyncTask} to run the requested operation in a background thread. This is
 * intended to be used for publishing events asynchronously by
 * {@link io.keen.client.android.AndroidKeenClient}.
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
class AsyncTaskExecutor implements Executor {

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(final Runnable command) {
        new AsyncTask<Void, Void, Void>() {
            @Override
            protected Void doInBackground(Void... voids) {
                command.run();
                return null;
            }
        }.execute();
    }

}