package io.keen.client.android;

import android.content.Context;
import android.os.AsyncTask;
import android.util.JsonWriter;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import io.keen.client.java.KeenClient;

/**
 * TODO: Documentation
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public class AndroidKeenInterfaces extends KeenClient.KeenClientInterfaces {

    private Context context;

    public AndroidKeenInterfaces(Context context) {
        // TODO: Is it right to use the application context? Probably but revisit this just in case.
        this.context = context.getApplicationContext();
        jsonHandler = new AndroidJsonWriter();
        eventStore = new FileEventStore();
        publishExecutor = new AsyncTaskExecutor();
    }

    private static final DateFormat ISO_8601_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    private static class AsyncTaskExecutor implements Executor {
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

    private static class AndroidJsonStreamWriter implements KeenClient.KeenJsonHandler {
        @Override
        public Map<String, Object> readJson(Reader reader) throws IOException {
            // TODO: Implement reading.
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public void writeJson(Writer writer, Map<String, ?> value) throws IOException {
            JsonWriter jsonWriter = new JsonWriter(writer);
            writeMap(jsonWriter, value);
        }

        private void writeMap(JsonWriter writer, Map<String, ?> map) throws IOException {
            writer.beginObject();
            for (Map.Entry entry : map.entrySet()) {
                // TODO: Make this more type-safe, and generally clean it up.
                writer.name((String) entry.getKey());
                writeValue(writer, entry.getValue());
            }
            writer.endObject();
        }

        private void writeArray(JsonWriter writer, Iterable<?> list) throws IOException {
            writer.beginArray();
            for (Object value : list) {
                writeValue(writer, value);
            }
            writer.endArray();
        }

        @SuppressWarnings("unchecked")
        private void writeValue(JsonWriter writer, Object value) throws IOException {
            // TODO: Handle null, Boolean, Integer, Long, Double
            if (value instanceof Map) {
                writeMap(writer, (Map<String, Object>) value);
            } else if (value instanceof Iterable) {
                writeArray(writer, (Iterable) value);
            } else if (value instanceof Calendar) {
                Date date = ((Calendar) value).getTime();
                String dateString = ISO_8601_FORMAT.format(date);
                writer.value(dateString);
            } else if (value instanceof String) {
                writer.value((String) value);
            } else {
                throw new UnsupportedOperationException("Unsupported value type " +
                        value.getClass().getCanonicalName());
            }
        }
    }

    private static final int COPY_BUFFER_SIZE = 4 * 1024;

    private static String readerToString(Reader reader) throws IOException {
        StringWriter writer = new StringWriter();
        char[] buffer = new char[COPY_BUFFER_SIZE];
        while (true) {
            int bytesRead = reader.read(buffer);
            if (bytesRead == -1) {
                break;
            } else {
                writer.write(buffer, 0, bytesRead);
            }
        }
        return writer.toString();
    }

    private static class AndroidJsonWriter implements KeenClient.KeenJsonHandler {
        @Override
        public Map<String, Object> readJson(Reader reader) throws IOException {
            String json = readerToString(reader);
            try {
                JSONObject jsonObject = new JSONObject(json);
                return JsonHelper.toMap(jsonObject);
            } catch (JSONException e) {
                throw new IOException(e);
            }
        }

        @Override
        public void writeJson(Writer writer, Map<String, ? extends Object> value) throws IOException {
            JSONObject jsonObject = new JSONObject(value);
            writer.write(jsonObject.toString());
        }
    }

    private class FileEventStore implements KeenClient.EventStore {
        @Override
        public OutputStream getCacheOutputStream(String eventCollection) throws IOException {
            File dir = getEventDirectoryForEventCollection(eventCollection);

            // make sure it exists
            createDirIfItDoesNotExist(dir);

            // now make sure we haven't hit the max number of events in this collection already
            File[] files = getFilesInDir(dir);
            if (files.length >= getMaxEventsPerCollection()) {
                // need to age out old data so the cache doesn't grow too large
                KeenLogging.log(String.format("Too many events in cache for %s, aging out old data", eventCollection));
                KeenLogging.log(String.format("Count: %d and Max: %d", files.length, getMaxEventsPerCollection()));

                // delete the eldest (i.e. first we have to sort the list by name)
                List<File> fileList = Arrays.asList(files);
                Collections.sort(fileList, new Comparator<File>() {
                    @Override
                    public int compare(File file, File file1) {
                        return file.getAbsolutePath().compareToIgnoreCase(file1.getAbsolutePath());
                    }
                });
                for (int i = 0; i < getNumberEventsToForget(); i++) {
                    File f = fileList.get(i);
                    if (!f.delete()) {
                        KeenLogging.log(String.format("CRITICAL: can't delete file %s, cache is going to be too big",
                                f.getAbsolutePath()));
                    }
                }
            }

            Calendar timestamp = Calendar.getInstance();
            File fileForEvent = getFileForEvent(eventCollection, timestamp);
            return new FileOutputStream(fileForEvent);
        }

        @Override
        public KeenClient.CacheEntries retrieveCached() throws IOException {
            File[] directories = getKeenCacheSubDirectories();

            Map<String, List<Map<String, Object>>> requestMap = null;
            Map<String, List<Object>> handleMap = new HashMap<String, List<Object>>();
            if (directories != null) {
                // this map will hold the eventual API request we send off to the Keen API
                requestMap = new HashMap<String, List<Map<String, Object>>>();

                // this map will hold references from a single directory to all its children

                // iterate through the directories
                for (File directory : directories) {
                    // get their files
                    File[] files = getFilesInDir(directory);
                    if (files != null) {

                        // build up the list of maps (i.e. events) based on those files
                        List<Map<String, Object>> requestList = new ArrayList<Map<String, Object>>();
                        // also remember what files we looked at
                        List<Object> handleList = new ArrayList<Object>();
                        for (File file : files) {
                            // iterate through the files, deserialize them from JSON, and then add them to the list
                            Map<String, Object> eventDict = readMapFromJsonFile(file);
                            requestList.add(eventDict);
                            handleList.add(file);
                        }

                        String collectionName = directory.getName();
                        if (!requestList.isEmpty()) {
                            requestMap.put(collectionName, requestList);
                        }
                        handleMap.put(collectionName, handleList);

                    } else {
                        KeenLogging.log("During upload the files list in the directory was null.");
                    }
                }
            }

            return new KeenClient.CacheEntries(handleMap, requestMap);
        }

        @Override
        public void removeFromCache(Object handle) throws IOException {
            if (!(handle instanceof File)) {
                throw new IllegalArgumentException("Expected File, but was " + handle.getClass());
            }

            File eventFile = (File) handle;
            if (!eventFile.delete()) {
                KeenLogging.log(String.format("CRITICAL ERROR: Could not remove event at %s",
                        eventFile.getAbsolutePath()));
            } else {
                KeenLogging.log(String.format("Successfully deleted file: %s", eventFile.getAbsolutePath()));
            }
        }
    }

    /* TODO: Re-implement upload capability.
     *
    synchronized private void uploadHelper(UploadFinishedCallback callback) {
        // iterate through all the sub-directories containing events in the Keen cache
        File[] directories = getKeenCacheSubDirectories();

        if (directories != null) {

            // this map will hold the eventual API request we send off to the Keen API
            Map<String, List<Map<String, Object>>> requestMap = new HashMap<String, List<Map<String, Object>>>();
            // this map will hold references from a single directory to all its children
            Map<File, List<File>> fileMap = new HashMap<File, List<File>>();

            // iterate through the directories
            for (File directory : directories) {
                // get their files
                File[] files = getFilesInDir(directory);
                if (files != null) {

                    // build up the list of maps (i.e. events) based on those files
                    List<Map<String, Object>> requestList = new ArrayList<Map<String, Object>>();
                    // also remember what files we looked at
                    List<File> fileList = new ArrayList<File>();
                    for (File file : files) {
                        // iterate through the files, deserialize them from JSON, and then add them to the list
                        Map<String, Object> eventDict = readMapFromJsonFile(file);
                        requestList.add(eventDict);
                        fileList.add(file);
                    }
                    if (!requestList.isEmpty()) {
                        requestMap.put(directory.getName(), requestList);
                    }
                    fileMap.put(directory, fileList);

                } else {
                    KeenLogging.log("During upload the files list in the directory was null.");
                }
            }

            // START HTTP REQUEST, WRITE JSON TO REQUEST STREAM
            try {
                if (!fileMap.isEmpty() && !requestMap.isEmpty()) { // could be empty due to inner null check above on files
                    HttpURLConnection connection = sendEvents(requestMap);


                    if (connection.getResponseCode() == 200) {
                        InputStream input = connection.getInputStream();
                        // if the response was good, then handle it appropriately
                        Map<String, List<Map<String, Object>>> responseBody = MAPPER.readValue(input,
                                new TypeReference<Map<String,
                                        List<Map<String,
                                                Object>>>>() {
                                });
                        handleApiResponse(responseBody, fileMap);
                    } else {
                        // if the response was bad, make a note of it
                        KeenLogging.log(String.format("Response code was NOT 200. It was: %d", connection.getResponseCode()));
                        InputStream input = connection.getErrorStream();
                        String responseBody = KeenUtils.convertStreamToString(input);
                        KeenLogging.log(String.format("Response body was: %s", responseBody));
                    }

                } else {
                    KeenLogging.log("No API calls were made because there were no events to upload");
                }
            } catch (JsonMappingException jsonme) {
                KeenLogging.log(String.format("ERROR: There was a JsonMappingException while sending %s to the Keen API: \n %s",
                        requestMap.toString(), jsonme.toString()));
            } catch (IOException e) {
                KeenLogging.log("There was an IOException while sending events to the Keen API: \n" + e.toString());
            }
        } else {
            KeenLogging.log("During upload the directories list was null, indicating a bad pathname.");
        }

        if (callback != null) {
            callback.callback();
        }

    }

    void handleApiResponse(Map<String, List<Map<String, Object>>> responseBody,
                           Map<File, List<File>> fileDict) {
        for (Map.Entry<String, List<Map<String, Object>>> entry : responseBody.entrySet()) {
            // loop through all the event collections
            String collectionName = entry.getKey();
            List<Map<String, Object>> results = entry.getValue();
            int count = 0;
            for (Map<String, Object> result : results) {
                // now loop through each event collection's individual results
                boolean deleteFile = true;
                boolean success = (Boolean) result.get(KeenConstants.SUCCESS_PARAM);
                if (!success) {
                    // grab error code and description
                    Map errorDict = (Map) result.get(KeenConstants.ERROR_PARAM);
                    String errorCode = (String) errorDict.get(KeenConstants.NAME_PARAM);
                    if (errorCode.equals(KeenConstants.INVALID_COLLECTION_NAME_ERROR) ||
                            errorCode.equals(KeenConstants.INVALID_PROPERTY_NAME_ERROR) ||
                            errorCode.equals(KeenConstants.INVALID_PROPERTY_VALUE_ERROR)) {
                        deleteFile = true;
                        KeenLogging.log("An invalid event was found. Deleting it. Error: " +
                                errorDict.get(KeenConstants.DESCRIPTION_PARAM));
                    } else {
                        String description = (String) errorDict.get(KeenConstants.DESCRIPTION_PARAM);
                        deleteFile = false;
                        KeenLogging.log(String.format("The event could not be inserted for some reason. " +
                                "Error name and description: %s %s", errorCode,
                                description));
                    }
                }

                if (deleteFile) {
                    // we only delete the file if the upload succeeded or the error means we shouldn't retry the
                    // event later
                    File eventFile = fileDict.get(getEventDirectoryForEventCollection(collectionName)).get(count);
                    if (!eventFile.delete()) {
                        KeenLogging.log(String.format("CRITICAL ERROR: Could not remove event at %s",
                                eventFile.getAbsolutePath()));
                    } else {
                        KeenLogging.log(String.format("Successfully deleted file: %s", eventFile.getAbsolutePath()));
                    }
                }
                count++;
            }
        }
    }
    */

    private Map<String, Object> readMapFromJsonFile(File jsonFile) {
        try {
            return jsonHandler.readJson(new FileReader(jsonFile));
        } catch (IOException e) {
            KeenLogging.log(String.format(
                    "There was an error when attempting to deserialize the contents of %s into JSON.",
                    jsonFile.getAbsolutePath()));
            e.printStackTrace();
            return null;
        }
    }

    /////////////////////////////////////////////
    // FILE IO
    /////////////////////////////////////////////

    File getDeviceCacheDirectory() {
        return context.getCacheDir();
    }

    private File getKeenCacheDirectory() {
        File file = new File(getDeviceCacheDirectory(), "keen");
        if (!file.exists()) {
            boolean dirMade = file.mkdir();
            if (!dirMade) {
                throw new RuntimeException("Could not make keen cache directory at: " + file.getAbsolutePath());
            }
        }
        return file;
    }

    private File[] getKeenCacheSubDirectories() {
        return getKeenCacheDirectory().listFiles(new FileFilter() { // Can return null if there are no events
            public boolean accept(File file) {
                return file.isDirectory();
            }
        });
    }

    File[] getFilesInDir(File dir) {
        return dir.listFiles(new FileFilter() {
            public boolean accept(File file) {
                return file.isFile();
            }
        });
    }

    File getEventDirectoryForEventCollection(String eventCollection) {
        File file = new File(getKeenCacheDirectory(), eventCollection);
        if (!file.exists()) {
            KeenLogging.log("Cache directory for event collection '" + eventCollection + "' doesn't exist. " +
                    "Creating it.");
            if (!file.mkdirs()) {
                KeenLogging.log("Can't create dir: " + file.getAbsolutePath());
            }
        }
        return file;
    }

    File[] getFilesForEventCollection(String eventCollection) {
        return getFilesInDir(getEventDirectoryForEventCollection(eventCollection));
    }

    private File getFileForEvent(String eventCollection, Calendar timestamp) {
        File dir = getEventDirectoryForEventCollection(eventCollection);
        int counter = 0;
        File eventFile = getNextFileForEvent(dir, timestamp, counter);
        while (eventFile.exists()) {
            eventFile = getNextFileForEvent(dir, timestamp, counter);
            counter++;
        }
        return eventFile;
    }

    private File getNextFileForEvent(File dir, Calendar timestamp, int counter) {
        long timestampInMillis = timestamp.getTimeInMillis();
        String name = Long.toString(timestampInMillis);
        return new File(dir, name + "." + counter);
    }

    private void createDirIfItDoesNotExist(File dir) {
        if (!dir.exists()) {
            assert dir.mkdir();
        }
    }
    /////////////////////////////////////////////

    // TODO: Clean up the event caching logic/constants/helper methods.

    // how many events can be stored for a single collection before aging them out
    static final int MAX_EVENTS_PER_COLLECTION = 10000;
    // how many events to drop when aging out
    static final int NUMBER_EVENTS_TO_FORGET = 100;
    private boolean isRunningTests;
    private int getMaxEventsPerCollection() {
        if (isRunningTests) {
            return 5;
        }
        return MAX_EVENTS_PER_COLLECTION;
    }

    private int getNumberEventsToForget() {
        if (isRunningTests) {
            return 2;
        }
        return NUMBER_EVENTS_TO_FORGET;
    }

}

