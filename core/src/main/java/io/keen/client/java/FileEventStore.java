package io.keen.client.java;

import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 */
public class FileEventStore implements KeenEventStore {

    private final File root;
    private final KeenJsonHandler jsonHandler;

    public FileEventStore(File root, KeenJsonHandler jsonHandler) throws IOException {
        this.root = root;
        this.jsonHandler = jsonHandler;
    }

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
    public CacheEntries retrieveCached() throws IOException {
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

        return new CacheEntries(handleMap, requestMap);
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

    private File getKeenCacheDirectory() {
        File file = new File(root, "keen");
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