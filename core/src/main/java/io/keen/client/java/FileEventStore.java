package io.keen.client.java;

import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of the {@link io.keen.client.java.KeenEventStore} interface using the file system
 * to cache events in between queueing and batch posting.
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public class FileEventStore implements KeenEventStore {

    ///// PUBLIC CONSTRUCTORS /////

    /**
     * Constructs a new File-based event store.
     *
     * @param root The root directory in which to store queued event files.
     * @param jsonHandler The JSON handler to use to write events to files, and to read them back.
     * @throws IOException If the provided {@code root} isn't an existing directory.
     */
    public FileEventStore(File root, KeenJsonHandler jsonHandler) throws IOException {
        if (!root.exists() || !root.isDirectory()) {
            throw new IOException("Event store root '" + root + "' must exist and be a directory");
        }

        this.root = root;
        this.jsonHandler = jsonHandler;
    }

    ///// PUBLIC METHODS /////

    /**
     * {@inheritDoc}
     */
    @Override
    public Object store(String eventCollection, Map<String, Object> event) throws IOException {
        // Prepare the collection cache directory.
        prepareCache(eventCollection);

        // Create the cache file.
        Calendar timestamp = Calendar.getInstance();
        File cacheFile = getFileForEvent(eventCollection, timestamp);

        // Write the event to the cache file.
        Writer writer = null;
        try {
            OutputStream out = new FileOutputStream(cacheFile);
            writer = new OutputStreamWriter(out, ENCODING);
            jsonHandler.writeJson(writer, event);
        } finally {
            KeenUtils.closeQuietly(writer);
        }

        // Return the file as the handle to use for retrieving/removing the event.
        return cacheFile;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> get(Object handle) throws IOException {
        if (!(handle instanceof File)) {
            throw new IllegalArgumentException("Expected File, but was " + handle.getClass());
        }

        File eventFile = (File) handle;
        return readMapFromJsonFile(eventFile);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void remove(Object handle) throws IOException {
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

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, List<Object>> getHandles() throws IOException {
        File[] directories = getKeenCacheSubDirectories();

        Map<String, List<Object>> handleMap = new HashMap<String, List<Object>>();
        if (directories != null) {
            // iterate through the directories
            for (File directory : directories) {
                String collectionName = directory.getName();
                File[] files = getFilesInDir(directory);
                if (files != null) {
                    List<Object> handleList = new ArrayList<Object>();
                    handleList.addAll(Arrays.asList(files));
                    handleMap.put(collectionName, handleList);
                } else {
                    KeenLogging.log("Directory was null while getting event handles: " + collectionName);
                }
            }
        }

        return handleMap;
    }

    ///// PRIVATE CONSTANTS /////

    /** The encoding to use when writing events to files. */
    private static final String ENCODING = "UTF-8";

    /** The number of events that can be stored for a single collection before aging them out. */
    private static final int MAX_EVENTS_PER_COLLECTION = 10000;

    /** The number of events to drop when aging out. */
    private static final int NUMBER_EVENTS_TO_FORGET = 100;

    ///// PRIVATE FIELDS /////

    private boolean isRunningTests;
    private final File root;
    private final KeenJsonHandler jsonHandler;

    ///// PRIVATE METHODS /////

    /**
     * Reads a file containing an event in JSON format and returns a {@link Map} representing that
     * object.
     *
     * @param jsonFile A file containing a JSON-formatted event.
     * @return A {@link java.util.Map} representing the event.
     */
    private Map<String, Object> readMapFromJsonFile(File jsonFile) {
        Reader reader = null;
        try {
            reader = new FileReader(jsonFile);
            return jsonHandler.readJson(reader);
        } catch (IOException e) {
            KeenLogging.log(String.format(
                    "There was an error when attempting to deserialize the contents of %s into JSON.",
                    jsonFile.getAbsolutePath()));
            e.printStackTrace();
            return null;
        } finally {
            KeenUtils.closeQuietly(reader);
        }
    }

    /**
     * Gets the root directory of the Keen cache, based on the root directory passed to the
     * constructor of this file store. If necessary, this method will attempt to create the
     * directory.
     *
     * @return The root directory of the cache.
     */
    private File getKeenCacheDirectory() throws IOException {
        File file = new File(root, "keen");
        if (!file.exists()) {
            boolean dirMade = file.mkdir();
            if (!dirMade) {
                throw new IOException("Could not make keen cache directory at: " + file.getAbsolutePath());
            }
        }
        return file;
    }

    /**
     * Gets an array containing all of the sub-directories (i.e. event collections) in the Keen
     * cache directory.
     *
     * @return An array of sub-directories.
     */
    private File[] getKeenCacheSubDirectories() throws IOException {
        return getKeenCacheDirectory().listFiles(new FileFilter() { // Can return null if there are no events
            public boolean accept(File file) {
                return file.isDirectory();
            }
        });
    }

    /**
     * Gets an array containing all of the files in the given directory.
     *
     * @param dir A directory.
     * @return An array containing all of the files in the given directory.
     */
    private File[] getFilesInDir(File dir) {
        return dir.listFiles(new FileFilter() {
            public boolean accept(File file) {
                return file.isFile();
            }
        });
    }

    /**
     * Gets the directory for events in the given collection.
     *
     * @param eventCollection The name of the event collection.
     * @return The directory containing events in the collection.
     */
    private File getEventDirectoryForEventCollection(String eventCollection) throws IOException {
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

    /**
     * Gets an array of all of the files for the given event collection.
     *
     * @param eventCollection The name of the event collection.
     * @return An array containing all of the files currently in the collection.
     */
    private File[] getFilesForEventCollection(String eventCollection) throws IOException {
        return getFilesInDir(getEventDirectoryForEventCollection(eventCollection));
    }

    /**
     * Gets the file to use for a new event in the given collection with the given timestamp. If
     * there are multiple events with identical timestamps, this method will use a counter to
     * create a unique file name for each.
     *
     * @param eventCollection The name of the event collection.
     * @param timestamp The timestamp of the event.
     * @return The file to use for the new event.
     */
    private File getFileForEvent(String eventCollection, Calendar timestamp) throws IOException {
        File dir = getEventDirectoryForEventCollection(eventCollection);
        int counter = 0;
        File eventFile = getNextFileForEvent(dir, timestamp, counter);
        while (eventFile.exists()) {
            eventFile = getNextFileForEvent(dir, timestamp, counter);
            counter++;
        }
        return eventFile;
    }

    /**
     * Gets the file to use for a new event in the given collection with the given timestamp,
     * using the provided counter.
     *
     * @param dir The directory in which the file should be created.
     * @param timestamp The timestamp to use as the base file name.
     * @param counter The counter to append to the file name.
     * @return The file to use.
     */
    private File getNextFileForEvent(File dir, Calendar timestamp, int counter) {
        long timestampInMillis = timestamp.getTimeInMillis();
        String name = Long.toString(timestampInMillis);
        return new File(dir, name + "." + counter);
    }

    /**
     * Creates the given directory, if it doesn't exist already. Otherwise does nothing.
     *
     * @param dir The directory to create.
     */
    private void createDirIfItDoesNotExist(File dir) {
        if (!dir.exists()) {
            boolean result = dir.mkdir();
            assert result;
        }
    }

    /**
     * Gets the maximum number of events per collection.
     *
     * @return The maximum number of events per collection.
     */
    private int getMaxEventsPerCollection() {
        if (isRunningTests) {
            return 5;
        }
        return MAX_EVENTS_PER_COLLECTION;
    }

    /**
     * Gets the number of events to discard if the maximum number of events is exceeded.
     *
     * @return The number of events to discard.
     */
    private int getNumberEventsToForget() {
        if (isRunningTests) {
            return 2;
        }
        return NUMBER_EVENTS_TO_FORGET;
    }

    /**
     * Prepares the file cache for the given event collection for another event to be added. This
     * method checks to make sure that the maximum number of events per collection hasn't been
     * exceeded, and if it has, this method discards events to make room.
     *
     * @param eventCollection The name of the event collection.
     * @throws IOException If there is an error creating the directory or validating/discarding
     * events.
     */
    private void prepareCache(String eventCollection) throws IOException {
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
    }

}