HTTP Sink Connector
===================

The HTTP sink connector allows you to export data from Kafka topics to HTTP based APIS.
The connector polls data from Kafka to write to the API based on the topics subscription.

Quick Start - Poor mans's Replicator
------------------------------------

.. include:: includes/prerequisites.rst


Confluent Replicator is a fully featured solution to replicate messages between topics and clusters. To see the basic
functionality of the HTTP connector, we'll be creating our own replicator using the HTTP Connector to produce messages
from a source topic to the REST Proxy instance in cp-demo.

---------------------------------
Create the source and sink topics
---------------------------------

Before we can replicate data we need to create source and destination topics and create some input data.
From inside a cp-demo broker container (``docker-compose exec kafka1 bash``):

#. Create topics:

    .. sourcecode:: bash

        kafka-topics --zookeeper zookeeper:2181 --topic jsontest.source --create --replication-factor 1 --partitions 1
        kafka-topics --zookeeper zookeeper:2181 --topic jsontest.replica --create --replication-factor 1 --partitions 1

#. Input some source data:

    .. sourcecode:: bash

        kafka-console-producer --broker-list localhost:10091 --topic jsontest.source
        >{"foo1":"bar1"}
        >{"foo2":"bar2"}
        >{"foo3":"bar3"}
        >{"foo4":"bar4"}
        >{"foo5":"bar5"}

#. Start a console consumer to monitor the output from the connector:

    .. sourcecode:: bash

        kafka-console-consumer --bootstrap-server localhost:10091 --topic jsontest.replica --from-beginning

----------------------------
Load the HTTP Sink Connector
----------------------------

Now we submit the HTTP connector to the cp-demo connect instance:

#.  From outside the container:

    .. sourcecode:: bash

        curl -X POST -H "Content-Type: application/json" --data '{ \
        "name": "http-sink", \
        "config": { \
                "connector.class":"uk.co.threefi.connect.http.HttpSinkConnector", \
                "tasks.max":"1", \
                "http.api.url":"https://restproxy:8086/topics/jsontest.replica", \
                "topics":"jsontest.source", \
                "request.method":"POST", \
                "headers":"Content-Type:application/vnd.kafka.json.v2+json|Accept:application/vnd.kafka.v2+json", \
                "value.converter":"org.apache.kafka.connect.storage.StringConverter", \
                "batch.prefix":"{\"records\":[", \
                "batch.suffix":"]}", \
                "batch.max.size":"5", \
                "regex.patterns":"^~$", \
                "regex.replacements":"{\"value\":~}", \
                "regex.separator":"~" }}' \
        http://localhost:8083/connectors

    Your output should resemble:

    .. sourcecode:: bash

        { \
        "name":"http-sink", \
        "config":{ \
                "connector.class":"uk.co.threefi.connect.http.HttpSinkConnector", \
                "tasks.max":"1", \
                "http.api.url":"https://restproxy:8086/topics/jsontest.replica", \
                "topics":"jsontest.source", \
                "request.method":"POST", \
                "headers":"Content-Type:application/vnd.kafka.json.v2+json|Accept:application/vnd.kafka.v2+json", \
                "value.converter":"org.apache.kafka.connect.storage.StringConverter", \
                "batch.prefix":"{\"records\":[", \
                "batch.suffix":"]}", \
                "batch.max.size":"5", \
                "regex.patterns":"^~$", \
                "regex.replacements":"{\"value\":~}", \
                "regex.separator":"~", \
                "name":"http-sink"}, \
        "tasks":[], \
        "type":null \
        }

.. tip:: Note the regex configurations. REST Proxy expects data to be wrapped in a structure as below:

    .. sourcecode:: bash

        {"records":[{"value":{"foo1":"bar1"}},{"value":{"foo2":"bar2"}}]}

    The regex configurations and batching parameters create this structure around the original messages.

-------------------
Confirm the results
-------------------

In your earlier opened console consumer you should see the following:

    .. sourcecode:: bash

        {"foo1":"bar1"}
        {"foo2":"bar2"}
        {"foo3":"bar3"}
        {"foo4":"bar4"}
        {"foo5":"bar5"}

.. tip:: In this example we have configured ``batch.max.size`` to 5. This means, if you produce more than 5 messages in
        a way in which connect will see them in a signle fetch (e.g. by producing them before starting the connector.
        You will see batches of 5 messages submitted as single calls to the HTTP API.

--------
Features
--------

-----------------------
Key/Topic substitutions
-----------------------

The special strings ``${key}`` and ``${topic}`` can be used in the http.api.url and regex.replacements property to
inject metadata into the destinationAPI.

------------------
Regex Replacements
------------------

The HTTP Sink connector can take a number of regex patterns and replacement strings that are applied the the message
before being submitted to the destination API. For more information see the configuration options ``regex.patterns``,
``regex.replacements`` and ``regex.separator``

--------
Batching
--------

The HTTP Sink connector batches up requests submitted to HTTP APIs for efficiency. Batches can be built with custom
separators, prefixes and suffixes. For more information see the configuration options ``batch.prefix``, ``batch.suffix``
and ``batch.separator``.

You can also control when batches are submitted with configuration for maximum size of a batch. For more information
see the configuration option ``batch.max.size``

All regex options mentioned above still apply when batching and will be applied to individual messages before being
submitted to the batch.
