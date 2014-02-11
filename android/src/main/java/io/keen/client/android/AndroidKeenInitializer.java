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

import io.keen.client.java.FileEventStore;
import io.keen.client.java.KeenClient;
import io.keen.client.java.KeenEventStore;
import io.keen.client.java.KeenJsonHandler;

/**
 * TODO: Documentation
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public class AndroidKeenInitializer {

    private final Context context;
    private KeenJsonHandler jsonHandler;
    private KeenEventStore eventStore;
    private Executor publishExecutor;
    private boolean isInitializeCalled;

    public AndroidKeenInitializer(Context context) {
        // TODO: Is it right to use the application context? Probably but revisit this just in case.
        this.context = context.getApplicationContext();
    }

    public synchronized void initialize(String projectId, String writeKey, String readKey) {
        if (isInitializeCalled) {
            throw new IllegalStateException("Initialize may only be called once");
        }

        if (jsonHandler == null) {
            jsonHandler = new AndroidJsonHandler();
        }

        if (eventStore == null) {
            try {
                eventStore = new FileEventStore(getDeviceCacheDirectory(), jsonHandler);
            } catch (IOException e) {
                // TODO: throw KeenInitializationException?
            }
        }

        if (publishExecutor == null) {
            publishExecutor = new AsyncTaskExecutor();
        }

        KeenClient.initialize(projectId, writeKey, readKey);
        isInitializeCalled = true;
    }

    public synchronized AndroidKeenInitializer withJsonHandler(KeenJsonHandler jsonHandler) {
        this.jsonHandler = jsonHandler;
        return this;
    }

    public synchronized AndroidKeenInitializer withEventStore(KeenEventStore eventStore) {
        this.eventStore = eventStore;
        return this;
    }

    public synchronized AndroidKeenInitializer withPublishExecutor(Executor publishExecutor) {
        this.publishExecutor = publishExecutor;
        return this;
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

    /////////////////////////////////////////////
    // FILE IO
    /////////////////////////////////////////////

    File getDeviceCacheDirectory() {
        return context.getCacheDir();
    }

}

