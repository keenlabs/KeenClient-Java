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

    public void onInitialize() {
        // do other stuff...

        // initialize the Keen Client with your Project Token.
        KeenClient.initialize(KEEN_PROJECT_TOKEN);
    }

The write key is required to send events to Keen IO - the read key is required to do analysis on Keen IO.

##### Send Events to Keen IO

Here’s a very basic example for an app that tracks "purchases":

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

##### Do analysis with Keen IO

    TODO

That's it! After running your code, check your Keen IO Project to see the event has been added.

### Changelog

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
