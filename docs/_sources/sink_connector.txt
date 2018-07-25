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
        >{"foo":"bar"}

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
        "config": {"connector.class":"uk.co.threefi.connect.http.HttpSinkConnector", \
                "tasks.max":"1", \
                "http.api.url":"https://restproxy:8086/topics/jsontest.replica", \
                "topics":"jsontest.source", \
                "request.method":"POST", \
                "headers":"Content-Type:application/vnd.kafka.json.v2+json|Accept:application/vnd.kafka.v2+json", \
                "value.converter":"org.apache.kafka.connect.storage.StringConverter", \
                "regex.patterns":"^~$", \
                "regex.replacements":"{\"records\":[{\"value\":~}]}", \
                "regex.separator":"~" }}' \
        http://localhost:8083/connectors

    Your output should resemble:

    .. sourcecode:: bash

        { \
        "name":"http-sink", \
        "config":{"connector.class":"uk.co.threefi.connect.http.HttpSinkConnector", \
                "tasks.max":"1", \
                "http.api.url":"https://restproxy:8086/topics/jsontest.replica", \
                "topics":"jsontest.source", \
                "request.method":"POST", \
                "headers":"Content-Type:application/vnd.kafka.json.v2+json|Accept:application/vnd.kafka.v2+json", \
                "value.converter":"org.apache.kafka.connect.storage.StringConverter", \
                "regex.patterns":"^~$", \
                "regex.replacements":"{\"records\":[{\"value\":~}]}", \
                "regex.separator":"~", \
                "name":"http-sink"}, \
        "tasks":[], \
        "type":null \
        }

.. tip:: Note the regex configurations. REST Proxy expects data to be wrapped in a structure as below:

    .. sourcecode:: bash

        {"records":[{"value":{"foo":"bar"}}]}

    The regex configurations add this structure to the beginning and end of the original message.

-------------------
Confirm the results
-------------------

In your earlier opened console consumer you should see the following:

    .. sourcecode:: bash

        {"foo":"bar"}

Features
--------

Key/Topic substitutions
^^^^^^^^^^^^^^^^^^^^^^^

The special strings ``${key}`` and ``${topic}`` can be used in the http.api.url and regex.replacements property to
inject metadata into the destinationAPI.

Regex Replacements
^^^^^^^^^^^^^^^^^^

The HTTP Sink connector can take a number of regex patterns and replacement strings that are applied the the message
before being submitted to the destination API. For more information see the configuration options ``regex.patterns``,
``regex.replacements`` and ``regex.separator``

