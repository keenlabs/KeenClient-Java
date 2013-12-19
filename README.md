Keen Java Client
===================

[![Build Status](https://travis-ci.org/keenlabs/KeenClient-Java.png?branch=master)](https://travis-ci.org/keenlabs/KeenClient-Java)

Use this library from any Java application where you want to record data using Keen IO.

[API Documentation](https://keen.io/docs/clients/java/usage-guide/)

[Client Documentation](https://keen.io/static/java-reference/index.html)

### Installation

Just drop the jar we've created into your project and configure the project to use it. We recommend having a "libs" directory to contain external dependencies, but it's up to you.

Download the jar [here](http://keen.io/static/code/KeenClient-Java.jar).

### Usage

To use this client with the Keen IO API, you have to configure your Keen IO Project ID and its access keys (if you need an account, [sign up here](https://keen.io/) - it's free).

##### Register Your Project ID and Access Keys

Setting a write key is required for publishing events. Setting a read key is required for running queries. The recommended way to set this configuration information is via the environment. The keys you can set are `KEEN_PROJECT_ID`, `KEEN_WRITE_KEY`, and `KEEN_READ_KEY`.

If you don't want to use environment variables for some reason, you can directly set values as follows:

```java
    public void onInitialize() {
        // do other stuff...

        // initialize the Keen Client
        KeenClient.initialize(KEEN_PROJECT_ID, KEEN_WRITE_KEY, KEEN_READ_KEY);
    }
```

##### Send Events to Keen IO

Here’s a very basic example for an app that tracks "purchases":

```java
    protected void track() {
        // create an event to upload to Keen
        Map<String, Object> event = new HashMap<String, Object>();
        event.put("item", "golden widget");

        // add it to the "purchases" collection in your Keen Project
        try {
            KeenClient.client().addEvent("purchases", event);
        } catch (KeenException e) {
            // handle the exception in a way that makes sense to you
            e.printStackTrace();
        }
    }
```

##### Do analysis with Keen IO

    TO DO

##### Generate a Scoped Key for Keen IO

Here's a simple method to generate a Scoped Write Key:

```java
    public String getScopedWriteKey(String apiKey) throws ScopedKeyException {
        Map<String, Object> options = new HashMap<String, Object>();
        options.put("allowed_operations", Arrays.asList("write"));
        return ScopedKeys.encrypt(apiKey, options);
    }
```

That's it! After running your code, check your Keen IO Project to see the event has been added.

### Changelog

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
