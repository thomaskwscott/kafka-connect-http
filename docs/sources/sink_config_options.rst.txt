.. _sink-config-options:

HTTP Sink Configuration Options
-------------------------------

Connection
^^^^^^^^^^

``http.api.url``
  HTTP API base url.

  * Type: string
  * Importance: high

``request.method``
  Request method used to interactr with the HTTP API (PUT/POST/DELETE).

  * Type: string
  * Default: POST
  * Importance: high

``headers``
  HTTP Request Headers.

  * Type: string
  * Default: null
  * Importance: low

``header.separator``
  Seperator used in the headers property.

  * Type: string
  * Default: |
  * Importance: low

Regex
^^^^^

``regex.patterns``
  Character separated list of regex patterns to match in the payload.

  * Type: string
  * Importance: low

``regex.replacements``
  Character separated list of string to use as replacements for the matches to the patterns in regex.patterns.

  * Type: string
  * Importance: low

``regex.separator``
  Character separator to use with regex.patterns and regex.replacements.

  * Type: string
  * Default: ~
  * Importance: low

Retries
^^^^^^^

``max.retries``
  The maximum number of times to retry on errors before failing the task.

  * Type: int
  * Default: 10
  * Valid Values: [0,...]
  * Importance: medium

``retry.backoff.ms``
  The time in milliseconds to wait following an error before a retry attempt is made.

  * Type: int
  * Default: 3000
  * Valid Values: [0,...]
  * Importance: medium

Batching
^^^^^^^^

``batch.key.pattern``
  Pattern used to build the key for a given batch. ${key} and ${topic} can be used to include message attributes here.

  * Type: string
  * Default: someKey
  * Importance: high

``batch.max.size``
  The number of records accumulated in a batch before the HTTP API will be invoked.

  * Type: int
  * Default: 1
  * Valid Values: [0,...]
  * Importance: high

``batch.prefix``
  Prefix added to record batches. This will be applied once at the beginning of the batch of records.

  * Type: string
  * Importance: high

``batch.suffix``
  Suffix added to record batches. This will be applied once at the end of the batch of records.

  * Type: string
  * Importance: high

``batch.seperator``
  Seperator for records in a batch.

  * Type: string
  * Default: ,
  * Importance: high

