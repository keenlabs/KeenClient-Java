Keen Java Clients
===================

[![Build Status](https://travis-ci.org/keenlabs/KeenClient-Java.png?branch=master)](https://travis-ci.org/keenlabs/KeenClient-Java)

The Keen Java clients enable you to record data using Keen IO from any Java application. The core library supports a variety of different paradigms for uploading events, including synchronous vs. asynchronous and single-event vs. batch. Different clients can be built on top of this core library to provide the right behaviors for a given platform or situation. The library currently includes a "plain" Java client and an Android client, but you can easily create your own by extending the base `KeenClient.Builder` class.

## Android Quick Start

Integrating Keen IO with an Android application? Check out the [Keen IO Android Sample App](https://github.com/keenlabs/KeenClient-Android-Sample) to get started, then check back here for more detailed documentation.

## Installation

You have several choices for how to include the Keen client in your Java application.

### Gradle

```groovy
repositories {
    mavenCentral()
}
dependencies {
    compile 'io.keen:keen-client-api-java:2.1.1'
}
```

For Android, use:

```groovy
    compile 'io.keen:keen-client-api-android:2.1.1@aar'
```

### Maven

Paste the following snippet into your pom.xml:

```xml
<dependency>
  <groupId>io.keen</groupId>
  <artifactId>keen-client-api-java</artifactId>
  <version>2.1.1</version>
</dependency>
```

For Android, replace the `artifactId` element with these two elements:

```xml
  <artifactId>keen-client-api-android</artifactId>
  <type>aar</type>
```

### JAR Download

Drop the appropriate jar into your project and configure the project to use it. We recommend having a "libs" directory to contain external dependencies, but it's up to you.

* ["Plain" Java Client](http://repo1.maven.org/maven2/io/keen/keen-client-api-java) - Note: This client depends on Jackson for JSON handling; you will need to ensure that the jackson-databind jar is on your classpath.
* [Android Client](http://repo1.maven.org/maven2/io/keen/keen-client-api-android) - Note: We publish both an AAR and a JAR; you may use whichever is more convenient based on your infrastructure and needs.
* [Core library only](http://repo1.maven.org/maven2/io/keen/keen-client-java-core) - This only includes an abstract client, so you will have to provide your own concrete implementation; see `JavaKeenClientBuilder` or `AndroidKeenClientBuilder` for examples.

### Build From Source

1. `git clone git@github.com:keenlabs/KeenClient-Java.git`
1. `cd KeenClient-Java`
1. `export JAVA_HOME=<path to Java>` (Windows: `set JAVA_HOME=<path to Java>`)
1. `./gradlew build` (Windows: `gradlew.bat build`)
1. Jars will be built and deposited into the various `build/libs` directories (e.g. `java/build/libs`, `android/build/libs`). You can then use these jars just as if you had downloaded them.

Note that this will also result in downloading and installing the Android SDK and various associated components. If you don't want/need the Keen Android library then you can simply remove `android` from the file `settings.gradle` in the root of the repository.

## Usage

### Building a Keen Client

A `KeenClient` object must be constructed via a `KeenClient.Builder` which specifies which implementations to use for each of the various abstraction interfaces (see below).

The Java and Android libraries each provide a `KeenClient.Builder` implementation with reasonable default behavior for the context. To use the plain Java builder:

```java
KeenClient client = new JavaKeenClientBuilder().build();
```

For the Android builder you must provide a `Context` such as your main `Activity` (the `Context` is used to access the device file system for event caching):

```java
KeenClient client = new AndroidKeenClientBuilder(this).build();
```

You may also define a custom builder, or override either of these builders' default behavior via the various `withXxx` methods.

### Using the KeenClient Singleton

As a convenience `KeenClient` includes an `initialize` method which sets a singleton member, allowing you to simply reference `KeenClient.client()` from anywhere in your application:

```java
// In some initialization logic:
KeenClient client = new JavaKeenClientBuilder().build();
KeenClient.initialize(client);
...
// In a totally separate piece of application logic:
KeenClient.client().addEvent(...);
```

Note that many people have [strong preferences against singletons](http://stackoverflow.com/questions/137975/what-is-so-bad-about-singletons). If you're one of them, feel free to ignore the `initialize` and `client` methods and manage your instance(s) explicitly.

### Specifying Your Project

To use the client with the Keen IO API, you have to configure your Keen IO Project ID and its access keys (if you need an account, [sign up here](https://keen.io/) - it's free).

In most scenarios you will always be adding events to the same project, so as a convenience the Keen client allows you to specify the project parameters in environment variables and those parameters will be implicitly used for all requests. The environment variables you should set are `KEEN_PROJECT_ID`, `KEEN_WRITE_KEY`, and `KEEN_READ_KEY`. Setting a write key is required for publishing events. Setting a read key is required for running queries.

#### Setting Default Project Explicitly

If you can't or prefer not to use environment variables, you can also set the default project explicitly:

```java
KeenProject project = new KeenProject(PROJECT_ID, WRITE_KEY, READ_KEY);
client.setDefaultProject(project);
```

#### Using Multiple Projects

If your use case requires multiple projects, you may define each project separately and provide a `KeenProject` object to each API call as you make it:

```java
public static final KeenProject PROJECT_A = new KeenProject(PROJECT_A_ID, PROJECT_A_WRITE_KEY, PROJECT_A_READ_KEY);
public static final KeenProject PROJECT_B = new KeenProject(PROJECT_B_ID, PROJECT_B_WRITE_KEY, PROJECT_B_READ_KEY);
...
KeenClient.client().addEvent(PROJECT_A, "collection", event, null, null);
```

#### Logging

By default, logging from ```KeenClient``` will be disabled and any Exceptions thrown will be caught and ignored. This is useful for two reasons:

* On development environments you may want to use a dummy API key.
* Your production application will never crash if an unchecked Exception is thrown.

If you want to re-enable logging then use the following statement after you create your ```KeenClient``` object:

```java
KeenLogging.enableLogging();
```

You can also explicitly disable Exception catching by using the following statement:

```java
KeenClient.client().setDebugMode(true);
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

NOTE: You are responsible for making sure that the contents of the event map can be properly serialized into JSON by the JSON handler you've configured the `KeenClient` to use. This shouldn't be an issue for standard maps of primitives and lists/arrays, but may be a problem for more complex data structures.

#### Single Event vs. Batch

To post events to the server one at a time, use the `addEvent` or `addEventAsync` methods.

To store events in a queue and periodically post all queued events in a single batch, use the `queueEvent` and `sendQueuedEvents` (or `sendQueuedEventsAsync`) methods.

#### Synchronous vs. Asynchronous

The `addEvent` and `sendQueuedEvents` methods will perform the entire HTTP request and response processing synchronously in the calling thread. Their `Async` counterparts will submit a task to the client's `publishExecutor`, which will execute it asynchronously.

#### Global Properties

To have a static set of properties automatically added to every event submitted, use the `setGlobalProperties` method:

```java
Map<String, Object> map = new HashMap<String, Object>();
map.put("some standard key", "some standard value");
client.setGlobalProperties(map);
```

To provide a dynamic set of properties at event creation time, use the `setGlobalPropertiesEvaluator` method:

```java
GlobalPropertiesEvaluator evaluator = new GlobalPropertiesEvaluator() {
    public Map<String, Object> getGlobalProperties(String eventCollection) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("some dynamic property name", "some dynamic property value");
        return map;
    }
};
client.setGlobalPropertiesEvaluator(evaluator);
```

#### Addons

[Addons](https://keen.io/docs/data-collection/data-enrichment/) may be applied by adding a list of addon maps to the `keenProperties` argument of `addEvent` or `queueEvent` (or their `Async` variants):

```java
Map<String, Object> event = new HashMap<String, Object>();
event.put("ip_address", "${keen.ip}");
Map<String, Object> keenProperties = new HashMap<String, Object>();
List<Object> addons = new ArrayList<Object>();
Map<String, Object> ipToGeo = new HashMap<String, Object>();
ipToGeo.put("name", "keen:ip_to_geo");
Map<String, Object> ipToGeoInput = new HashMap<String, Object>();
ipToGeoInput.put("ip", "ip_address");
ipToGeo.put("input", ipToGeoInput);
ipToGeo.put("output", "ip_geo_info");
addons.add(ipToGeo);
keenProperties.put("addons", addons);
KeenClient.client().queueEvent("android-sample-button-clicks", event, keenProperties);
```

#### Building Event Maps

You may use whatever means you find most convenient to construct the event `Map` objects that you provide to the Keen client. However, building the maps individually may become tedious (particularly if your events have deeply-nested properties). Using the Google Guava `ImmutableMap.Builder` class can tidy things up a bit; for example:

```java
final Map<String, Object> m = ImmutableMap.<String, Object>builder().
        put("foo", 10).
        put("bar", "some_value").
        put("nested", ImmutableMap.<String, Object>builder().
                put("a", true).
                put("b", 17).
                build()).
        build();
```

#### Using Callbacks

By default the library assumes that your events are "fire and forget", that is, you don't need to know when (or even if) they succeed. However if you do need to know for some reason, the client includes overloads of each method which take a `KeenCallback` object. This object allows you to receive notification when a request completes, as well as whether it succeeded and, if it failed, an `Exception` indicating the cause of the failure.

### Do analysis with Keen IO

    TO DO

### Generate a Scoped Key for Keen IO

Here's a simple example of generating a Scoped Write Key:

```java
    String masterApiKey = "YOUR_KEY_HERE"
    Map<String, Object> filter = new HashMap<String, Object>();
    List<Map<String, Object>> filters = new ArrayList<Map<String, Object>>();
    Map<String, Object> options = new HashMap<String, Object>();

    filter.put("property_name", "user_id");
    filter.put("operator", "eq");
    filter.put("property_value", "123");

    filters.add(filter);

    options.put("allowed_operations", Arrays.asList("write"));
    options.put("filters", filters);

    ScopedKeys.encrypt(masterApiKey, options);
```

### Publish Executor Lifecycle Management

By default both the Java and Android clients use an `ExecutorService` to perform asynchronous requests, and you may wish to manage its life-cycle. For example:

```java
    ExecutorService service = (ExecutorService) KeenClient.client().getPublishExecutor();
    service.shutdown();
    service.awaitTermination(5, TimeUnit.SECONDS);
```

Note that once you've shut down the publish executor for a given client, there is no way to restart or replace that executor. You will need to build a new client.

### Using an HTTP proxy

The KeenClient supports HTTP proxies via the `setProxy(String proxyHost, int proxyPort)` and `setProxy(Proxy proxy)` methods of a `KeenClient` instance. Simply use one of those methods after building a client like so:

```java
KeenClient client = new JavaKeenClientBuilder().build();
client.setProxy("secureproxy.example.com", 2570);
// now use the client object as you normally would
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

* `HttpHandler`: This interface provides an abstraction around executing HTTP requests.
* `KeenJsonHandler`: The client uses an instance of this interface to serialize and de-serialize JSON objects. This allows the caller to use whatever JSON library is most convenient in their environment, without requiring a specific (and possibly large) library.
* `KeenEventStore`: This interface is used to store events in between `queueEvent` and `sendQueuedEvents` calls. The library comes with two implementations:
  * `RamEventStore`: Stores events in memory. This is fast but not persistent.
  * `FileEventStore`: Stores events in the local file system. This is persistent but needs to be provided with a working directory that is safe to use across application restarts.
* `Executor`: The client uses an `Executor` to perform all of the various `*Async` operations. This allows callers to configure thread pools and control shutdown behavior, if they so desire.

### Overriding Default Interfaces

If you want to use a custom implementation of any of the abstraction interfaces described above, you can do so with the appropriate Builder methods. For example:

```java
MyEventStore eventStore = new MyEventStore(...);
JavaKeenClient client = new JavaKeenClient.Builder()
        .withEventStore(eventStore)
        .build();
```

### State of the Build

As of 2.0.2 the following non-critical issues are present in the build:

* If you do not have the Android SDK documentation installed, you will see a warning in the build output for the 'android:javadocRelease' task. This can be resolved by running the Android SDK manager and installing the "Documentation for Android SDK" package. There is no need to bother unless you care about the built Javadoc.

## Client-Specific Considerations

### Android Client

If using the Android Keen client, you will need to make sure that your application has the `INTERNET` permission. If it’s not already present, add it to your AndroidManifest.xml file. The entry below should appear inside the `<manifest>` tag.

    <uses-permission android:name="android.permission.INTERNET"/>

##### FAQs

Q: What happens when the device is offline? Will events automatically be sent when the device connects to wifi again?

A: Our SDK handles offline data collection and have built-in limits to prevent too much data from building up. We also handle re-posting events so that you don't have to worry about this.

Here's how it works. You specify when events should be uploaded to Keen (e.g. when the app is backgrounded).

If your player is offline when that happens, their data will be collected on the device and it will not be posted to Keen IO.
However, the next time they trigger the code that send events (e.g. backgrounding the app again) all the data from the previous sessions will also be posted (the timestamps will reflect the times the events actually happened).

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

#### InvalidEventException: "An event cannot contain a root-level property named 'keen'."

Your event maps can't contain properties in the keen namespace directly. If you want to add properties to the keen namespace (such as to override the timestamp or apply add-ons) you must use the `keenProperties` parameter to `queueEvent`/`addEvent`:

```java
Map<String, Object> event = new HashMap<String, Object>();
event.put("property", "value");
Map<String, Object> keenProperties = new HashMap<String, Object>();
keenProperties.put("timestamp", "2014-11-01T12:00:00.000Z");
client.addEvent("collection-name", event, keenProperties);
```

## Changelog

##### 2.1.1

+ Fixed bug that caused extra/corrupted events to be sent.

##### 2.1.0

+ In Android SDK, check for network connectivity before attempting to POST.
+ Limit the number of times a failed event will be retried.
+ Updated version of KeenCallback with more information included.

##### 2.0.3

+ Added HTTP Proxy support
+ Minor bugfixes

##### 2.0.2

+ Fixed bug which caused older versions of Android (pre-KitKat) to be unable to post events.
+ Enabled building with Java 6 (to ensure backwards compatibility).

##### 2.0.1

+ Minor bugfix to address issue with logging in Android.

##### 2.0.0

+ Refactored Java and Android SDKs into a shared core library and two different implementations of the `KeenClient.Builder` class.

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

If you have any questions, bugs, or suggestions, please use Github Issues section of this repo! That's the fastest way to get a response from the people who know this library best :).

### Contributing
This is an open source project and we love involvement from the community! Hit us up with pull requests and issues.
