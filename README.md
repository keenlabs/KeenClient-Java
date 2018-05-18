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
    compile 'io.keen:keen-client-api-java:5.2.0'
}
```

For Android, use:

```groovy
    compile 'io.keen:keen-client-api-android:5.2.0@aar'
```

### Maven

Paste the following snippet into your pom.xml:

```xml
<dependency>
  <groupId>io.keen</groupId>
  <artifactId>keen-client-api-java</artifactId>
  <version>5.2.0</version>
</dependency>
```

For Android, replace the `artifactId` element with these two elements:

```xml
  <artifactId>keen-client-api-android</artifactId>
  <type>aar</type>
```

### JAR Download

**Warning:** This approach is not recommended as it forces you to deal with getting all the
right transitive dependencies. We highly encourage you to use a dependency management
framework such as Maven :)

To use JARs directly just drop the appropriate JARs into your project and configure the project
to use it. It is conventional to create a "libs" directory to contain external dependencies, but it's up to you.

* [Core library](http://repo1.maven.org/maven2/io/keen/keen-client-java-core) - This only includes an abstract client, so you will have to provide your own concrete implementation; see `JavaKeenClientBuilder` or `AndroidKeenClientBuilder` for examples.
* ["Plain" Java Client](http://repo1.maven.org/maven2/io/keen/keen-client-api-java) - Dependencies: Core plus the Jackson library for JSON handling.
* [Android Client](http://repo1.maven.org/maven2/io/keen/keen-client-api-android) - Dependencies: Core. Note: We publish both an AAR and a JAR; you may use whichever is more convenient based on your infrastructure and needs.
* [Query Client](http://repo1.maven.org/maven2/io/keen/keen-client-api-query) - Dependencies: Core, "Plain" Java, and a suitable JSON library.

### Build From Source

1. `git clone git@github.com:keenlabs/KeenClient-Java.git`
1. `cd KeenClient-Java`
1. `export JAVA_HOME=<path to Java>` (Windows: `set JAVA_HOME=<path to Java>`)
1. `export ANDROID_HOME=<path to Android SDK location - tested with Platform version 23 and Build Tools 23.0.3>`
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

#### Property Merging

Global properties will be merged with per-event properties.  The merge order is, from lowest to highest priority:
static globals, dynamic globals, then per-event properties.  Keen properties in globals will be separated from non-keen
properties and merged with the event keenProperties parameter in the same manner.

Note: this is not a deep merge.  Only the top level key-value pairs will be merged, duplicate keys will replace
previous ones in order of priority.

See an example of the property merging in action in [KeenClientTest](core/src/test/java/io/keen/client/java/KeenClientTest.java)
method `testPropertyMergeOrder()`

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

The query capabilities within the Java Keen client enable you to send POST queries and receive the results of the queries in a JSON object. For query types, refer to [API technical reference](https://keen.io/docs/api/reference/).

#### Add the Keen Query Client Package to your Build

The Query Client is published into a separate artifact, since many applications only need event publishing. If you would like to use the query client then you will need to ensure that you also have the appropriate artifact in your build. The instructions are the same as described above under [Installation](#installation), but with the artifact name `keen-client-api-query` (instead of `keen-client-api-java` or `keen-client-api-android`).

#### Building a Keen Query Client

You can build a `KeenQueryClient` by just providing a `KeenProject`. Note that for query purposes, the write key is not required. It is therefore OK and normal to provide ```null``` argument for the write key, unless that same `KeenProject` will be used for publishing events as well.
```java
KeenProject queryProject = new KeenProject("<project id>", "<write key>", "<read key>");
KeenQueryClient queryClient = new KeenQueryClient.Builder(queryProject).build();
```
Optionally, users can also specify a HTTP Handler, base URL, or JSON Handler:
```java
KeenQueryClient queryClient = new KeenQueryClient.Builder(queryProject)
        .withHttpHandler(httpHandler)
        .withJsonHandler(jsonHandler)
        .withBaseUrl(baseURL)
        .build();
```
#### Using the KeenQueryClient to send Queries

The most simple way that users can use the `KeenQueryClient` to send queries is as follows. These methods take only the required query parameters as input, and the user receives a very specific ```long``` or ```double``` response type. Please note that Timeframe is now required by the Keen IO API.
```java
long count = queryClient.count("<event_collection>", new RelativeTimeframe("this_week"));
long countUnique = queryClient.countUnique("<event_collection>", "<target_property>", new AbsoluteTimeframe("2015-05-15T19:00:00.000Z","2015-06-07T19:00:00.000Z"));
double minimum = queryClient.minimum("<event_collection>", "<target_property>", new RelativeTimeframe("this_week"));
double maximum = queryClient.maximum("<event_collection>", "<target_property>", new RelativeTimeframe("this_week"));
double average = queryClient.average("<event_collection>", "<target_property>", new RelativeTimeframe("this_week"));
double median = queryClient.median("<event_collection>", "<target_property>", new RelativeTimeframe("this_week"));
double percentile = queryClient.percentile("<event_collection>", "<target_property>", new RelativeTimeframe("this_week"));
double sum = queryClient.sum("<event_collection>", "<target_property>", new RelativeTimeframe("this_week"));
```
The exceptions are Select Unique, Extraction, Funnel, and Multi-Analysis queries. These queries are a little more complicated, and Extraction is currently not supported by the Keen Query Client.

#### Advanced

Alternatively, users can use optional parameters to send queries. The return type is a `QueryResult` object. The user is expected to verify the expected `QueryResult` subclass, given the parameters entered.
```java
Query query = new Query.Builder(QueryType.COUNT)
        .withEventCollection("<event_collection>")
        .withTimeframe(new RelativeTimeframe("this_month"))
        .build();
QueryResult result = queryClient.execute(query);
if (result.isLong()) {
    long countValue = result.longValue();
    // do something with countValue
}
```

Some special cases are when "Group By" and "Interval" are specified, as well as the Select Unique query.

Select Unique queries return a list of unique values, given the target property. Therefore, the `QueryResult` will be a list of unique property values. The `QueryResult` type only supports Integer, Double, String, and List values; therefore, if the property value is not one of the aforementioned types, then you may not be able to access that value.

``` java
Query query = new Query.Builder(QueryType.SELECT_UNIQUE)
        .withEventCollection("<event_collection>")
        .withTargetProperty("click-number")
        .withTimeframe(new RelativeTimeframe("this_month"))
        .build();
QueryResult result = queryClient.execute(query);
if (result.isListResult()) {
    List<QueryResult> listResults = result.getListResults();
    for (QueryResult item : listResults) {
        if (item.isLong()) {
            // do something with long value
        }
    }
}
```

Specifying "Group By" in the query will cause the query response to be a GroupByResult object. This object stores Map<Group, QueryResult> objects, where the Group contains the unique property/value pairs.

``` java
Query query = new Query.Builder(QueryType.COUNT)
        .withEventCollection("<event_collection>")
        .withGroupBy("click-number")
        .withTimeframe(new RelativeTimeframe("this_month"))
        .build();
QueryResult result = queryClient.execute(query);
if (result.isGroupResult()) {
    for (Map.Entry<Group, QueryResult> groupResult : result.getGroupResults().entrySet()) {
        Map<String, Object> groupProperies = groupResult.getKey().getProperties();
        long groupCount = groupResult.getValue().longValue();
        // ... do something with the group properties and the count result
    }
}
```

Specifying "Interval" in the query will cause the query response to be an `IntervalResult` object. An `IntervalResult` is a type of `QueryResult` that consist of `Map<AbsoluteTimeframe,QueryResult>` objects.

``` java
Query query = new Query.Builder(QueryType.COUNT)
        .withEventCollection("<event_collection>")
        .withInterval("weekly")
        .withTimeframe(new RelativeTimeframe("this_month"))
        .build();
QueryResult result = queryClient.execute(query);
if (result.isIntervalResult()) {
    for (IntervalResultValue intervalResult : result.getIntervalResults()) {
        AbsoluteTimeframe timeframe = intervalResult.getTimeframe();
        long intervalCount = intervalResult.getResult().longValue();
        // ... do something with the absolute timeframe and count result.
    }
}
```
Filtering via both Group By and Interval will cause the query response to be an `IntervalResult` object that contains `GroupByResult` objects follows:

``` java
Query query = new Query.Builder(QueryType.COUNT)
        .withEventCollection("<event_collection>")
        .withInterval("weekly")
        .withGroupBy("click-number")
        .withTimeframe(new RelativeTimeframe("this_month"))
        .build();
QueryResult result = queryClient.execute(query);

if (result.isIntervalResult()) {
    for (IntervalResultValue intervalResult : result.getIntervalResults()) {
        AbsoluteTimeframe timeframe = intervalResult.getTimeframe();

        for (Map.Entry<Group, QueryResult> groupResult : intervalResult.getResult().getGroupResults().entrySet()) {
            Map<String, Object> groupProperies = groupResult.getKey().getProperties();
            long groupCount = groupResult.getValue().longValue();
            // ... do something with the group properties and the count result
        }
    }
}
```

#### Multi-Analysis

To perform a [Multi-Analysis](https://keen.io/docs/api/#multi-analysis), use the `MultiAnalysis.Builder` instead. An instance of `MultiAnalysisResult` will be returned unless Group By and/or Interval parameters are included, in which case the `MultiAnalysisResult`(s) will be nested inside an `IntervalResult` and/or a `GroupByResult`, just like any other `QueryResult` for other single analysis types when grouping/intervals are applied:

``` java
final MultiAnalysis multiAnalysis = new MultiAnalysis.Builder()
        .withEventCollection("the_collection")
        .withTimeframe(new RelativeTimeframe("this_month"))
        .withSubAnalysis(new SubAnalysis("label_for_count", QueryType.COUNT))
        .withSubAnalysis(new SubAnalysis("sum_analysis_label", QueryType.SUM, "property_to_sum"))
        .build();

QueryResult result = this.queryClient.execute(multiAnalysis);

if (result instanceof MultiAnalysisResult) {
    MultiAnalysisResult multiAnalysisResult = (MultiAnalysisResult)result;

    for (String subAnalysisLabel : multiAnalysisResult.getAllResults().keySet()) {
        QueryResult resultForSubAnalysis = multiAnalysisResult.getResultFor(subAnalysisLabel);
        // ... do something with the results of the various sub-analyses.
    }
}
```
The `MultiAnalysis.Builder` will only allow configuration of properties that are actually supported by a Multi-Analysis, and will throw an exception at the build() call if the set of parameters configured isn't sufficient, e.g. if there are no `SubAnalysis` instances set.

#### Funnel Analysis

Funnel analysis can similarly be performed using the `Funnel.Builder`. As with multi-analysis, some parameter checking is done to ensure required parameters are at least provided. The result of a funnel analysis is a `FunnelResult`, on which a `ListResult` containing the results of the funnel are available through `getFunnelResult()`, and if actor values were requested, their results will be available through `getActorsResult()`. `FunnelStep`s are required to provide a collection name, an actor property name, and a `Timeframe` instance unless one is provided for the entire funnel using `Funnel.Builder.withTimeframe()`. Additional optional parameters are available for specifying a list of `Filter`s for each step and the special parameters `inverted`, `optional`, and `withActors`.

``` java
final Funnel funnel = new Funnel.Builder()
        .withStep(new FunnelStep("signed_up", "visitor.guid", new RelativeTimeframe("this_7_days")))
        .withStep(new FunnelStep("completed_profile", "user.guid", new RelativeTimeframe("this_7_days")))
        .withStep(new FunnelStep("referred_user", "user.guid", new RelativeTimeframe("this_7_days", "UTC")))
        .build();

QueryResult result = this.queryClient.execute(funnel);

if (result instanceof FunnelResult) {
    // The result was a FunnelResult as expected.
    // Cast the result to the appropriate type.
    FunnelResult funnelResult = (FunnelResult)result;
    // Get the sub-result for the funnel analysis
    ListResult funnelListResult = funnelResult.getFunnelResult();
    // Unpack the list of QueryResults for each funnel step
    List<QueryResult> funnelResultData = funnelListResult.getListResults();
    // Iterate through each funnel step result
    for (QueryResult stepResult : funnelResultData) {
        if (stepResult instanceof LongResult) {
            // Do something with each result of the funnel.
            long stepData = stepResult.longValue();
        }
    }

    // Get the actors result, which may be null.
    // In the case of this example, no steps requested actor values
    // so it indeed would be null, but FunnelStep has an optional parameter
    // to request actor values which will populate this result.
    ListResult actorsResult = funnelResult.getActorsResult();
    if (null != result.getActorsResult()) {
        // A list of actor values was provided in the response.
        // Unpack the list of lists of actors
        List<QueryResult> actorsResultLists = actorsResult.getListResults();
        for (QueryResult stepActorsResult : actorsResultLists) {
            // Get the list of actors for this step
            List<QueryResult> stepActorsResultList = stepActorsResult.getListResults();
            // Iterate through all actor values
            for (QueryResult actorResult : stepActorsResultList) {
                // Unpack the actor value
                if (actorResult instanceof StringResult) {
                    String actorValue = actorResult.stringValue();
                }
                else if (actorResult instanceof LongResult) {
                    long actorValue = actorResult.longValue();
                }
            }
        }
    }
}

```

#### Saved/Cached Queries

To work with [Saved/Cached Queries](https://keen.io/docs/api/#saved-queries), create a 
`KeenQueryClient` as normal, then use it to create a `SavedQueries` implementation:

```java
KeenQueryClient queryClient = ...;
SavedQueries savedQueryApi = queryClient.getSavedQueriesInterface();
```

One can create a query, get a single query definition by resource name, get all query definitions, 
and delete a query. Note that most Saved/Cached Query functionality requires a Master Key, but 
retrieving result(s) of a query can be done with just a Read Key. Here are some examples:

```java

// First, we'll create a query. Let's count the number of purchases with a price >= $1 in the last
// two weeks including the current week.
SingleAnalysis count = new SingleAnalysis.Builder(QueryType.COUNT)
        .withEventCollection("purchases")
        .withFilter("price", FilterOperator.GREATER_THAN_EQUAL, 1.00)
        .withTimeframe(new RelativeTimeframe("this_2_weeks"))
        .build();

// There are variations of create*Query() to set things like the refresh rate.
Map<String, Object> rawCreateResponse = savedQueryApi.createSavedQuery("saved-query-name", count);

// Get all query definitions for this project.
List<Map<String, Object>> allQueryDefs = savedQueryApi.getAllDefinitions();

// Get the query definition for the query we just created.
Map<String, Object> countQueryDef = savedQueryApi.getDefinition("saved-query-name");

// Now retrieve the result of the query actually having been run.
QueryResult sumResultSaved = savedQueryApi.getResult("saved-query-name");

// Maybe update the query. See notes below, as this is a little more complex.

// Now delete the query.
savedQueryApi.deleteQuery("saved-query-name");
```

Updating queries requires sending a `PUT` with the complete query definition. Any property not sent 
is interpreted as being cleared/removed. This means that properties set via another client, 
including the Projects Explorer Web UI, will be lost this way.

The updateQuery() method makes this easier by allowing client code to just specify the properties 
that need updating. To do this, it will retrieve the existing query definition first, which means 
there will be two HTTP requests. Use updateQueryFull() in code that already has a full query 
definition that can reasonably be expected to be current.

There are also some helper methods to update a few properties.

```java
Map<String, Object> updateResponse = null;

// Update a saved query to now be a cached query with the minimum refresh rate of 4 hrs...

// ...using partial update:
Map<String, Object> partialUpdates = new HashMap<String, Object>();
int refreshRate = 4 * 3600; // 4 hrs
partialUpdates.put("refresh_rate", refreshRate);

updateResponse = savedQueryApi.updateQuery("saved-query-name", partialUpdates);

// ...using full update, if we've already fetched the query definition and removed unacceptable
// properties. Some properties, like "run_information" returned by getting a query definition cannot
// be `PUT` back or an error is returned.:
Map<String, Object> fullUpdates = mySanitizeHelper(countQueryDef);
fullUpdates.put("refresh_rate", 14400);
updateResponse = savedQueryApi.updateQueryFull("saved-query-name", fullUpdates);

// ...or using the helpers:
updateResponse = savedQueryApi.setRefreshRate("saved-query-name", RefreshRate.fromHours(4));


// Update a saved query to a new resource name...

// ...using partial update:
Map<String, Object> partialUpdatesName = new HashMap<String, Object>();
partialUpdatesName.put("query_name", "cached-query-name");
updateResponse = savedQueryApi.updateQuery("saved-query-name", partialUpdates);

// ...using full update, if we've already fetched the query definition or have it lying around for
// whatever reason. We send "refresh_rate" again, along with the entire definition, or else it would
// be reset. Assume "updateResponse" is the response from having changed the refresh rate just now:
Map<String, Object> fullUpdatesName = mySanitizeHelper(updateResponse);
fullUpdatesName.put("query_name", "cached-query-name");
updateResponse = savedQueryApi.updateQueryFull("saved-query-name", fullUpdatesName);

// ...or using the helpers:
updateResponse = savedQueryApi.setQueryName("saved-query-name", "cached-query-name");
```

As one can see, updating with partial updates or using the helpers where available is easier than 
re-creating a full query definition which contains only acceptable properties. In either case, the 
updates are presented as a `Map<String, ?>` that mirrors the JSON structure of the query definition, 
like what is returned from `getDefinition()`. So if client code just wants to change the 
"event_collection" to "sales" instead, it has to pass a nested `Map<>` structure. As the updates 
get more deeply nested or otherwise complicated, it might be better to just re-create the query 
from scratch, optionally deleting it first. The raw response from `create*Query()` has a field 
indicating whether the query was created or updated.

```java

// Updating a deep property, like the "event_collection" property.

// Construct a nested `Map<>` structure matching JSON { "query": { "event_collection": "sales" } }
Map<String, Object> queryUpdates = HashMap<String, Object>();
queryUpdates.put("event_collection", "sales");
Map<String, Object> updateEventCollection = new HashMap<String, Object>();
updateEventCollection.put("query", queryUpdates);

updateResponse = savedQueryApi.updateQuery("cached-query-name", updateEventCollection);
```

### Utility Methods

There are also some utility methods to add filters and timeframes to a `Query`:
```java

// this will add two filter parameters, with 1 < click-count < 5
Query query = new Query.Builder(QueryType.COUNT)
        .withEventCollection("<event_collection>")
        .withFilter("click-count", FilterOperator.GREATER_THAN, 1)
        .withFilter("click-count", FilterOperator.LESS_THAN, 5)
        .withTimeframe(new RelativeTimeframe("this_month"))
        .build();

QueryResult result = queryClient.execute(query);
if (result.isLong()) {
    long queryResult = result.longValue();
}
```

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

Note that if using an authenticated proxy, it's also necessary to set the [default authenticator](https://docs.oracle.com/javase/7/docs/api/java/net/Authenticator.html#setDefault(java.net.Authenticator)):

```java
import java.net.*;

Authenticator.setDefault(
   new Authenticator() {
      @Override
      public PasswordAuthentication getPasswordAuthentication() {
         return new PasswordAuthentication("user", "password".toCharArray());
      }
   }
);
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
* [Java 8 Unlimited Strength Jurisdiction Policy Files](http://www.oracle.com/technetwork/java/javase/downloads/jce8-download-2133166.html)

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

##### 5.2.0

+ Add Saved/Cached Query support.
+ Support includes Create, Read a definition or all definitions, Retrieve results, Update partial or
  complete definitions, and Delete of Saved/Cached Queries.

##### 5.1.0

+ Add Multi-Analysis and Funnel capabilities to KeenQueryClient.
+ Multi-Analysis support includes MultiAnalysis, a Builder, SubAnalysis and MultiAnalysisResult.
+ Some refactoring to share code common to MultiAnalysis and SingleAnalysis.
+ SingleAnalysis does not yet replace the Query class, but will soon.
+ Timezone parameter is now built into RelativeTimeframe.
+ Query filters are now an unordered collection of instances of the new Filter class.
+ Query.Builder.setFilters() now copies the given Maps into a collection of Filter instances.

##### 5.0.2

+ Add "Keen-Sdk" header for version tracking.
+ At merge time, an issue with Travis CI downloading Gradle over HTTPS caused
  the CI build to fail on openjdk6, but those tests were run and do pass.

##### 5.0.1

+ Fixed bug in packaging; artifacts now correctly include transitive dependencies.

##### 5.0.0

+ Changed packaging to no longer bundle core classes into client artifacts.
+ Minor changes to accommodate Android unit testing.

##### 4.0.0

+ Change interface for queries with interval results.
+ Give JacksonJsonHandler public access.
+ Fix incorrect access for RelativeTimeframe class.
+ Fix NPE bug with a valid query result that returns null.

##### 3.0.0

+ Merge event properties & keen properties in order of priority.

##### 2.2.0

+ Fix bug with scoped key generation not working with newer Keen projects.

##### 2.1.2

+ Fixed bug which caused crash in Google App Engine
+ Added alpha version of query support

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

### Questions & Support

If you have any questions, bugs, or suggestions, please use Github Issues section of this repo! That's the fastest way to get a response from the people who know this library best :).

### Contributing
This is an open source project and we love involvement from the community! Hit us up with pull requests and issues.
