package io.keen.client.java;

/**
 * Encapsulation of a single Keen project, including read/write keys.
 *
 * @author Kevin Litwack
 * @since 2.0.0
 */
public class KeenProject {

    ///// PUBLIC CONSTRUCTORS /////

    public KeenProject() {
        this(new Environment());
    }

    KeenProject(Environment env) {
        this(env.getKeenProjectId(), env.getKeenWriteKey(), env.getKeenReadKey());
    }

    /**
     * Construct a Keen project.
     *
     * @param projectId The Keen IO Project ID.
     * @param writeKey  Your Keen IO Write Key. This may be null if this project will only be used
     *                  for reading events.
     * @param readKey   Your Keen IO Read Key. This may be null if this project will only be used
     *                  for writing events.
     */
    public KeenProject(String projectId, String writeKey, String readKey) {
        if (projectId == null || projectId.length() == 0) {
            throw new IllegalArgumentException("Invalid project id specified: " + projectId);
        }

        this.projectId = projectId;
        this.writeKey = writeKey;
        this.readKey = readKey;
    }

    ///// PUBLIC METHODS /////

    /**
     * Getter for the Keen Project Id associated with this instance of the {@link KeenClient}.
     *
     * @return the Keen Project Id
     */
    public String getProjectId() {
        return projectId;
    }

    /**
     * Getter for the Keen Read Key associated with this instance of the {@link KeenClient}.
     *
     * @return the Keen Read Key
     */
    public String getReadKey() {
        return readKey;
    }

    /**
     * Getter for the Keen Write Key associated with this instance of the {@link KeenClient}.
     *
     * @return the Keen Write Key
     */
    public String getWriteKey() {
        return writeKey;
    }

    ///// PRIVATE FIELDS /////

    private final String projectId;
    private final String readKey;
    private final String writeKey;

}
