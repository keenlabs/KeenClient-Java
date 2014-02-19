package io.keen.client.android;

import android.os.AsyncTask;

import java.util.concurrent.Executor;

/**
 * DOCUMENT
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