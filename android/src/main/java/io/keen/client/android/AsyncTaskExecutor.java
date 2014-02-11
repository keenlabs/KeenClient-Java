package io.keen.client.android;

import android.os.AsyncTask;

import java.util.concurrent.Executor;

/**
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 */
class AsyncTaskExecutor implements Executor {

    @Override
    public void execute(final Runnable command) {
        // TODO: Make sure there is a way to get errors out.
        new AsyncTask<Void, Void, Void>() {
            @Override
            protected Void doInBackground(Void... voids) {
                command.run();
                return null;
            }
        }.execute();
    }

}