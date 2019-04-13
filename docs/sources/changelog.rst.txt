.. _http_connector_changelog:

Changelog
=========

Version 5.2.1
-------------

* Removed batch.linger.ms property to prevent potential scenario where messages can have offsets committed without being sent during task failure.

Version 5.2.0
-------------

* Changed versioning to match tested Confluent Platform version
* Added batching options

Version 1.0.0
-------------