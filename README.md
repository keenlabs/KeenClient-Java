Keen Java Clients
===================

[![Build Status](https://travis-ci.org/keenlabs/KeenClient-Java.png?branch=master)](https://travis-ci.org/keenlabs/KeenClient-Java)

The Keen Java clients enable you to record data using Keen IO from any Java application. The core library supports a variety of different paradigms for uploading events, including synchronous vs. asynchronous and single-event vs. batch. Different clients can be built on top of this core library to provide the right behaviors for a given platform or situation. The library currently includes a ["plain" Java client ](#plain-java) and an [Android client](#android), but you can easily create your own by extending the base `KeenClient` class.

## Installation

You have several choices for how to include the Keen client in your Java application.

**NOTE: The 2.0.0 libraries have not yet been published to Maven. This README will be updated once they're published. Until then, please follow the instructions to [build from source](#build from source).**

### Gradle

```groovy
repositories {
    mavenCentral()
}
dependencies {
    compile 'io.keen:keen-client-api-java:2.0.0'
    compile 'io.keen:keen-client-api-android:2.0.0'
}
```

### Maven

Paste the following snippet into your pom.xml:

```xml
<dependency>
  <groupId>io.keen</groupId>
  <artifactId>keen-client-api-java</artifactId>
  <artifactId>keen-client-api-android</artifactId>
  <version>2.0.0</version>
</dependency>
```

### JAR Download

Drop the appropriate jar into your project and configure the project to use it. We recommend having a "libs" directory to contain external dependencies, but it's up to you.

**NOTE: As mentioned, the 2.0.0 jars are not posted yet. Stay Tuned.**

* ["Plain" Java Client](http://repo1.maven.org/maven2/io/keen/keen-client-api-java) - Note: This client depends on Jackson for JSON handling; you will need to ensure that the jackson-databind jar is on your classpath.
* [Android Client](http://repo1.maven.org/maven2/io/keen/keen-client-api-android)
* [Core library only](http://repo1.maven.org/maven2/io/keen/keen-client-api-core) - This does not include a usable client implementation, so you will have to provide your own.

### Build From Source

1. `git clone git@github.com:keenlabs/KeenClient-Java.git`
1. `cd KeenClient-Java`
1. Do you want/need to build the Android client?
  1. Yes, I want to build Android: Configure the Android SDK path in the file `android/local.properties`. The path should be to the root of the Android SDK, i.e. the folder containing the "platforms" directory. Note also that the build logic is currently hard-coded to use android-19; if you don't have SDK version 19, update line 35 of `android\build.gradle` to your SDK version. The format of the file is simply:

      sdk.dir=[Android SDK path]

  1. No, I don't need to build Android: Remove the `android` project from the `include` directive in `settings.gradle`.
1. `export JAVA_HOME=<path to Java>` (Windows: `set JAVA_HOME=<path to Java>`)
1. `./gradlew build` (Windows: `gradlew.bat build`)
1. Jars will be built and deposited into the various `build/libs` directories (e.g. `java/build/libs`, `android/build/libs`). You can then use these jars just as if you had downloaded them.

## Usage

### Initialize the Keen Client

Before you can use the client you must initialize it. The exact method of initialization depends on which concrete `KeenClient` implementation you use. For the plain Java client:

```java
JavaKeenClient.initialize();
```

For the Android client you must provide a `Context` such as your main `Activity` (the `Context` is used to access the device file system for event caching):

```java
AndroidKeenClient.initialize(this);
```

For custom clients make sure that the `KeenClient` subclass calls `KeenClient#initialize` and passes an instance. See the source for `JavaKeenClient` or `AndroidKeenClient` for an example.

### Specifying Your Project

To use the client with the Keen IO API, you have to configure your Keen IO Project ID and its access keys (if you need an account, [sign up here](https://keen.io/) - it's free).

In most scenarios you will always be adding events to the same project, so as a convenience the Keen client allows you to specify the project parameters in environment variables and those parameters will be implicitly used for all requests. The environment variables you should set are `KEEN_PROJECT_ID`, `KEEN_WRITE_KEY`, and `KEEN_READ_KEY`. Setting a write key is required for publishing events. Setting a read key is required for running queries.

#### Setting Default Project Explicitly

If you can't or prefer not to use environment variables, you can also set the default project explicitly:

```java
KeenProject project = new KeenProject(PROJECT_ID, WRITE_KEY, READ_KEY);
KeenClient.client().setDefaultProject(project);
```

#### Using Multiple Projects

If your use case requires multiple projects, you may define each project separately and provide a `KeenProject` object to each API call as you make it:

```java
public static final KeenProject PROJECT_A = new KeenProject(PROJECT_A_ID, PROJECT_A_WRITE_KEY, PROJECT_A_READ_KEY);
public static final KeenProject PROJECT_B = new KeenProject(PROJECT_B_ID, PROJECT_B_WRITE_KEY, PROJECT_B_READ_KEY);
...
KeenClient.client().addEvent(PROJECT_A, "collection", event, null, null);
```

### Send Events to Keen IO

Here's a very basic example for an app that tracks "purchases":

```java
    protected void track() {
        // Create an event to upload to Keen.
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("item", "golden widget");

        // Add it to the "purchases" collection in your Keen Project.
        KeenClient.client().addEvent("purchases", event);
    }
```

That's it! After running your code, check your Keen IO Project to see the event has been added.

#### Single Event vs. Batch

To post events to the server one at a time, use the `addEvent` or `addEventAsync` methods.

To store events in a queue and periodically post all queued events in a single batch, use the `queueEvent` and `sendQueuedEvents` (or `sendQueuedEventsAsync`) methods.

#### Synchronous vs. Asynchronous

The `addEvent` and `sendQueuedEvents` methods will perform the entire HTTP request and response processing synchronously in the calling thread. Their `Async` counterparts will submit a task to the client's `publishExecutor`, which will execute it asynchronously.

#### Using Callbacks

By default the library assumes that your events are "fire and forget", that is, you don't need to know when (or even if) they succeed. However if you do need to know for some reason, the client includes overloads of each method which take a `KeenCallback` object. This object allows you to receive notification when a request completes, as well as whether it succeeded and, if it failed, an `Exception` indicating the cause of the failure.

### Do analysis with Keen IO

    TO DO

### Generate a Scoped Key for Keen IO

Here's a simple method to generate a Scoped Write Key:

```java
    public String getScopedWriteKey(String apiKey) throws ScopedKeyException {
        Map<String, Object> options = new HashMap<String, Object>();
        options.put("allowed_operations", Arrays.asList("write"));
        return ScopedKeys.encrypt(apiKey, options);
    }
```

## Working with the Source

### Using IntelliJ IDEA or Android Studio

After cloning this repository you can quickly get started with an IntelliJ or Android Studio project by running:

`./gradlew idea`

This will generate all of the necessary project files.

### Design Principles

* Minimize external dependencies
  * In environments where jar size is important, Keen client should be as small as possible.
* Never cause an application crash
  * In the default configuration, the library should always swallow exceptions silently.
  * During development and testing, `setDebugMode(true)` causes client to fail fast.
  * If production code needs to know when requests succeed or fail, use callbacks.
* Provide flexible control over when and how events are uploaded
  * Synchronous vs. asynchronous (with control over the asynchronous mechanism)
  * Single-event vs. batch (with control over how events are cached in between batch uploads)

### KeenClient Interfaces

The `KeenClient` base class relies on three interfaces to abstract out behaviors which specific client implementations may wish to customize:

* `KeenJsonHandler`: The client uses an instance of this interface to serialize and de-serialize JSON objects. This allows the caller to use whatever JSON library is most convenient in their environment, without requiring a specific (and possibly large) library.
* `KeenEventStore`: This interface is used to store events in between `queueEvent` and `sendQueuedEvents` calls. The library comes with two implementations:
  * `RamEventStore`: Stores events in memory. This is fast but not persistent.
  * `FileEventStore`: Stores events in the local file system. This is persistent but needs to be provided with a working directory that is safe to use across application restarts.
* `Executor`: The client uses an `Executor` to perform all of the various `*Async` operations. This allows callers to configure thread pools and control shutdown behavior, if they so desire.

## Client-Specific Considerations

### Java Client

The Java client uses an `ExecutorService` to perform asynchronous requests, and you may wish to manage its life-cycle. For simple management, `JavaKeenClient` provides the methods `shutdownPublishExecutorService` and `restartPublishExecutorService` which you can call directly:

```java
    JavaKeenClient javaClient = (JavaKeenClient) KeenClient.client();
    javaClient.shutdownPublishExecutorService(0);
```

For more control you can get a reference to the `ExecutorService` itself:

```java
    ExecutorService service = (ExecutorService) KeenClient.client().getPublishExecutor();
    service.awaitTermination(5, TimeUnit.SECONDS);
```

### Android Client

If using the Android Keen client, you will need to make sure that your application has the `INTERNET` permission. If it’s not already present, add it to your AndroidManifest.xml file. The entry below should appear inside the `<manifest>` tag.

    <uses-permission android:name="android.permission.INTERNET"/>

## Troubleshooting

#### "Unable to find location of Android SDK. Please read documentation" error during build

If you are not trying to build the Android client, you can remove `android` from the list of included projects in `settings.gradle`. Otherwise you need to create the file `android/local.properties` with the following line:

        sdk.dir=<Android SDK path>

#### "RuntimeException: Stub!" error in JUnit tests

This is usually caused by the Android SDK being before JUnit in your classpath. (Android includes a stubbed version of JUnit.) To fix this, move JUnit ahead of the Android SDK so it gets picked up first.

#### "java.security.InvalidKeyException: Illegal key size or default parameters" error in JUnit tests or using Scoped Keys

The default encryption settings for JDK6+ don't allow using AES-256-CBC, which is the encryption methodology used for Keen IO Scoped Keys. To fix, download the appropriate file policy files:

* [Java 6 Unlimited Strength Jurisdiction Policy Files](http://www.oracle.com/technetwork/java/javase/downloads/jce-6-download-429243.html)
* [Java 7 Unlimited Strength Jurisdiction Policy Files](http://www.oracle.com/technetwork/java/javase/downloads/jce-7-download-432124.html)

Follow the install instructions and scoped key generation should work. Note that the policy files will need to be installed on any device which runs your application, or scoped key generation will result in a runtime exception.

## Changelog

##### 2.0.0

+ Refactored Java and Android SDKs into a shared core library and two different implementations
of the `KeenClient` class.

##### 1.0.7

+ Make Maven happy.

##### 1.0.6

+ Support changing base URL for API (mostly to support disabling SSL).

##### 1.0.5

+ Support reading Project ID and access keys from environment variables.

##### 1.0.4

+ Fix bug with padding in Scoped Keys implementation.

##### 1.0.3

+ Add Scoped Keys implementation.

##### 1.0.2

+ Bugfix from 1.0.1 to actually use write key when adding events.

##### 1.0.1

+ Changed project token -> project ID.
+ Added support for read and write scoped keys.

### To Do

* Support analysis APIs.

### Questions & Support

If you have any questions, bugs, or suggestions, please
report them via Github Issues. Or, come chat with us anytime
at [users.keen.io](http://users.keen.io). We'd love to hear your feedback and ideas!

### Contributing
This is an open source project and we love involvement from the community! Hit us up with pull requests and issues.
