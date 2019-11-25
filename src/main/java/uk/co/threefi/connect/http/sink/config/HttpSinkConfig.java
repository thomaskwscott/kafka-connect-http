/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.co.threefi.connect.http.sink.config;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;

public class HttpSinkConfig extends AbstractConfig {

    public enum RequestMethod {
        PUT,
        POST,
        DELETE;
    }

    public static final String HTTP_API_URL = "http.api.url";
    private static final String HTTP_API_URL_DOC = "HTTP API URL.";
    private static final String HTTP_API_URL_DISPLAY = "HTTP URL";

    public static final String REQUEST_METHOD = "request.method";
    private static final String REQUEST_METHOD_DOC = "HTTP Request Method.";
    private static final String REQUEST_METHOD_DISPLAY = "Request Method";
    private static final String REQUEST_METHOD_DEFAULT = RequestMethod.POST.toString();

    public static final String HEADERS = "headers";
    private static final String HEADERS_DOC = "character separated HTTP headers. Default "
            + "separator is |, use header.separator to modify this.";
    private static final String HEADERS_DISPLAY = "headers";
    private static final String HEADERS_DEFAULT = "";

    public static final String HEADER_SEPERATOR = "header.separator";
    private static final String HEADER_SEPERATOR_DOC = "separator character used in "
            + "headers property.";
    private static final String HEADER_SEPERATOR_DISPLAY = "header separator";
    private static final String HEADER_SEPERATOR_DEFAULT = "\\|";

    public static final String REGEX_PATTERNS = "regex.patterns";
    private static final String REGEX_PATTERNS_DOC = "character seperated regex patterns "
            + "to match for replacement in the destination messages";
    private static final String REGEX_PATTERNS_DISPLAY = "regex patterns";
    private static final String REGEX_PATTERNS_DEFAULT = "";

    public static final String REGEX_REPLACEMENTS = "regex.replacements";
    private static final String REGEX_REPLACEMENTS_DOC = "character seperated regex replacements "
            + "to use with the patterns in regex.patterns. ${key} and ${topic} can be used here.";
    private static final String REGEX_REPLACEMENTS_DISPLAY = "regex replacements";
    private static final String REGEX_REPLACEMENTS_DEFAULT = "";

    public static final String REGEX_SEPARATOR = "regex.separator";
    private static final String REGEX_SEPARATOR_DOC = "separator character used in "
            + "regex.patterns and regex.replacements property.";
    private static final String REGEX_SEPARATOR_DISPLAY = "regex separator";
    private static final String REGEX_SEPARATOR_DEFAULT = "~";

    public static final String MAX_RETRIES = "max.retries";
    private static final int MAX_RETRIES_DEFAULT = 10;
    private static final String MAX_RETRIES_DOC =
            "The maximum number of times to retry on errors before failing the task.";
    private static final String MAX_RETRIES_DISPLAY = "Maximum Retries";

    public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
    private static final int RETRY_BACKOFF_MS_DEFAULT = 3000;
    private static final String RETRY_BACKOFF_MS_DOC =
            "The time in milliseconds to wait following an error before a retry attempt is made.";
    private static final String RETRY_BACKOFF_MS_DISPLAY = "Retry Backoff (millis)";

    public static final String BATCH_KEY_PATTERN = "batch.key.pattern";
    private static final String BATCH_KEY_PATTERN_DEFAULT = "someKey";
    private static final String BATCH_KEY_PATTERN_DOC = "pattern used to build the key for a given batch. "
            + " ${key} and ${topic} can be used to include message attributes here.";
    private static final String BATCH_KEY_PATTERN_DISPLAY = "Batch key pattern";

    public static final String BATCH_MAX_SIZE = "batch.max.size";
    private static final int BATCH_MAX_SIZE_DEFAULT = 1;
    private static final String BATCH_MAX_SIZE_DOC =
            "The number of records accumulated in a batch before the HTTP API will be invoked.";
    private static final String BATCH_MAX_SIZE_DISPLAY = "Maximum batch size";

    public static final String BATCH_PREFIX = "batch.prefix";
    private static final String BATCH_PREFIX_DEFAULT = "";
    private static final String BATCH_PREFIX_DOC = "prefix added to record batches. "
            + "this will be applied once at the beginning of the batch of records.";
    private static final String BATCH_PREFIX_DISPLAY = "Batch prefix";

    public static final String BATCH_SUFFIX = "batch.suffix";
    private static final String BATCH_SUFFIX_DEFAULT = "";
    private static final String BATCH_SUFFIX_DOC = "suffix added to record batches. "
            + "this will be applied once at the end of the batch of records.";
    private static final String BATCH_SUFFIX_DISPLAY = "Batch suffix";

    private static final String BATCH_SEPARATOR = "batch.separator";
    private static final String BATCH_SEPARATOR_DEFAULT = ",";
    private static final String BATCH_SEPARATOR_DOC = "Separator for records in a batch.";
    private static final String BATCH_SEPARATOR_DISPLAY = "Batch separator";

    public static final String BATCH_BODY_PREFIX = "batch.body.prefix";
    private static final String BATCH_BODY_PREFIX_DEFAULT = "";
    private static final String BATCH_BODY_PREFIX_DOC = "Prefix for the body of the payload being sent";
    private static final String BATCH_BODY_PREFIX_DISPLAY = "Batch body prefix";

    public static final String BATCH_BODY_SUFFIX = "batch.body.suffix";
    private static final String BATCH_BODY_SUFFIX_DEFAULT = "";
    private static final String BATCH_BODY_SUFFIX_DOC = "Suffix for the body of the payload being sent";
    private static final String BATCH_BODY_SUFFIX_DISPLAY = "Batch body suffix";

    public static final String BATCH_BODY_FIELD_FILTER = "batch.body.field.filter";
    private static final String BATCH_BODY_FIELD_FILTER_DEFAULT = "";
    private static final String BATCH_BODY_FIELD_FILTER_DOC = "Comma-separated list with the names of fields " +
            "we want to filter out from the body of each sub-request";
    private static final String BATCH_BODY_FIELD_FILTER_DISPLAY = "Batch body field filter list";

    private static final String CONNECTION_GROUP = "Connection";
    private static final String RETRIES_GROUP = "Retries";
    private static final String REGEX_GROUP = "Regex";
    private static final String BATCHING_GROUP = "Batching";
    private static final String SALESFORCE_AUTH_GROUP = "Salesforce Auth";
    private static final String CONVERTER_GROUP = "Converters";

    public static final String SALESFORCE_AUTHENTICATION_ROOT = "salesforce.authentication.root";
    private static final String SALESFORCE_AUTHENTICATION_ROOT_DEFAULT = "https://test.salesforce.com";
    private static final String SALESFORCE_AUTHENTICATION_ROOT_DOC = "The Root URL for the Salesforce Authentication server, without a trailing slash.";
    private static final String SALESFORCE_AUTHENTICATION_ROOT_DISPLAY = "Salesforce Authentication Server Root URL path";

    private static final String SALESFORCE_AUTHENTICATION_CLIENT_ID = "salesforce.authentication.client_id";
    private static final String SALESFORCE_AUTHENTICATION_CLIENT_ID_DEFAULT = "";
    private static final String SALESFORCE_AUTHENTICATION_CLIENT_ID_DOC = "OAuth client_id for the connected app for which the certificate was registered";
    private static final String SALESFORCE_AUTHENTICATION_CLIENT_ID_DISPLAY = "Salesforce Authentication client_id";

    private static final String SALESFORCE_AUTHENTICATION_USERNAME = "salesforce.authentication.username";
    private static final String SALESFORCE_AUTHENTICATION_USERNAME_DEFAULT = "";
    private static final String SALESFORCE_AUTHENTICATION_USERNAME_DOC = "The username of the Salesforce user.";
    private static final String SALESFORCE_AUTHENTICATION_USERNAME_DISPLAY = "Salesforce Authentication username";

    public static final String SALESFORCE_AUTHENTICATION_PRIVATE_KEY = "salesforce.authentication.private_key";
    private static final String SALESFORCE_AUTHENTICATION_PRIVATE_KEY_DEFAULT = "";
    private static final String SALESFORCE_AUTHENTICATION_PRIVATE_KEY_DOC = "The private key used to sign the login request.";
    private static final String SALESFORCE_AUTHENTICATION_PRIVATE_KEY_DISPLAY = "Salesforce Authentication Private Key";

    public static final String RESPONSE_TOPIC = "response.topic";
    private static final String RESPONSE_TOPIC_DEFAULT = "";
    private static final String RESPONSE_TOPIC_DOC = "The response topic to use for all responses";
    private static final String RESPONSE_TOPIC_DISPLAY = "Response Topic";
    public static final String RESPONSE_PRODUCER = "response.producer.";

    public static final String RESPONSE_BODY = "response.body";
    private static final String RESPONSE_BODY_DEFAULT = "";
    private static final String RESPONSE_BODY_DOC = "The name for the body to be found in a response";
    private static final String RESPONSE_BODY_DISPLAY = "Response Body";

    public static final String ERROR_TOPIC = "error.topic";
    private static final String ERROR_TOPIC_DEFAULT = "";
    private static final String ERROR_TOPIC_DOC = "The error topic to hold all error cases from the application";
    private static final String ERROR_TOPIC_DISPLAY = "Error Topic";
    public static final String ERROR_PRODUCER = "error.producer.";

    public static final String ERROR_BATCH_RESPONSE_STATUS_CODE = "error.batch.response.status_code";
    private static final String ERROR_BATCH_RESPONSE_STATUS_CODE_DEFAULT = "";
    private static final String ERROR_BATCH_RESPONSE_STATUS_CODE_DOC = "The name for the status code to be found in a batch error response";
    private static final String ERROR_BATCH_RESPONSE_STATUS_CODE_DISPLAY = "Error Batch Response Status Code";

    public static final String ERROR_BATCH_RESPONSE_KEY = "error.batch.response.key";
    private static final String ERROR_BATCH_RESPONSE_KEY_DEFAULT = "";
    private static final String ERROR_BATCH_RESPONSE_KEY_DOC = "The name for the key to be found in a batch error response";
    private static final String ERROR_BATCH_RESPONSE_KEY_DISPLAY = "Error Batch Response Key";

    public static final String ERROR_BATCH_RESPONSE_BODY = "error.batch.response.body";
    private static final String ERROR_BATCH_RESPONSE_BODY_DEFAULT = "";
    private static final String ERROR_BATCH_RESPONSE_BODY_DOC = "The name for the body to be found in a batch error response";
    private static final String ERROR_BATCH_RESPONSE_BODY_DISPLAY = "Error Batch Response Body";

    public static final String KEY_CONVERTER = "key.converter";
    private static final String KEY_CONVERTER_DEFAULT = "org.apache.kafka.connect.storage.StringConverter";
    private static final String KEY_CONVERTER_DOC = "The key converter JAVA class";
    private static final String KEY_CONVERTER_DISPLAY = "Key converter class";

    public static final String VALUE_CONVERTER = "value.converter";
    private static final String VALUE_CONVERTER_DEFAULT = "org.apache.kafka.connect.storage.StringConverter";
    private static final String VALUE_CONVERTER_DOC = "The value converter JAVA class";
    private static final String VALUE_CONVERTER_DISPLAY = "Value converter class";

    public static final String VALUE_CONVERTER_SR_URL = "value.converter.schema.registry.url";
    private static final String VALUE_CONVERTER_SR_URL_DEFAULT = "";
    private static final String VALUE_CONVERTER_SR_URL_DOC = "The URL of the schema registry for the Value Converter";
    private static final String VALUE_CONVERTER_SR_URL_DISPLAY = "Value Converter Schema Registry URL";


    private static final ConfigDef.Range NON_NEGATIVE_INT_VALIDATOR = ConfigDef.Range.atLeast(0);


    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            // Connection
            .define(
                    HTTP_API_URL,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    HTTP_API_URL_DOC,
                    CONNECTION_GROUP,
                    1,
                    ConfigDef.Width.LONG,
                    HTTP_API_URL_DISPLAY
            )

            // Retries
            .define(
                    MAX_RETRIES,
                    ConfigDef.Type.INT,
                    MAX_RETRIES_DEFAULT,
                    NON_NEGATIVE_INT_VALIDATOR,
                    ConfigDef.Importance.MEDIUM,
                    MAX_RETRIES_DOC,
                    RETRIES_GROUP,
                    1,
                    ConfigDef.Width.SHORT,
                    MAX_RETRIES_DISPLAY
            )
            .define(
                    RETRY_BACKOFF_MS,
                    ConfigDef.Type.INT,
                    RETRY_BACKOFF_MS_DEFAULT,
                    NON_NEGATIVE_INT_VALIDATOR,
                    ConfigDef.Importance.MEDIUM,
                    RETRY_BACKOFF_MS_DOC,
                    RETRIES_GROUP,
                    2,
                    ConfigDef.Width.SHORT,
                    RETRY_BACKOFF_MS_DISPLAY
            )
            .define(
                    REQUEST_METHOD,
                    ConfigDef.Type.STRING,
                    REQUEST_METHOD_DEFAULT,
                    EnumValidator.in(RequestMethod.values()),
                    ConfigDef.Importance.HIGH,
                    REQUEST_METHOD_DOC,
                    CONNECTION_GROUP,
                    2,
                    ConfigDef.Width.MEDIUM,
                    REQUEST_METHOD_DISPLAY
            )
            .define(
                    HEADERS,
                    ConfigDef.Type.STRING,
                    HEADERS_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    HEADERS_DOC,
                    CONNECTION_GROUP,
                    3,
                    ConfigDef.Width.MEDIUM,
                    HEADERS_DISPLAY
            )
            .define(
                    HEADER_SEPERATOR,
                    ConfigDef.Type.STRING,
                    HEADER_SEPERATOR_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    HEADER_SEPERATOR_DOC,
                    CONNECTION_GROUP,
                    4,
                    ConfigDef.Width.SHORT,
                    HEADER_SEPERATOR_DISPLAY
            )
            .define(
                    RESPONSE_TOPIC,
                    ConfigDef.Type.STRING,
                    RESPONSE_TOPIC_DEFAULT,
                    Importance.LOW,
                    RESPONSE_TOPIC_DOC,
                    CONNECTION_GROUP,
                    5,
                    Width.LONG,
                    RESPONSE_TOPIC_DISPLAY
            )
            .define(
                  RESPONSE_BODY,
                  ConfigDef.Type.STRING,
                  RESPONSE_BODY_DEFAULT,
                  Importance.LOW,
                  RESPONSE_BODY_DOC,
                  CONNECTION_GROUP,
                  6,
                  Width.LONG,
                  RESPONSE_BODY_DISPLAY
            )
            .define(
                    ERROR_TOPIC,
                    ConfigDef.Type.STRING,
                    ERROR_TOPIC_DEFAULT,
                    Importance.LOW,
                    ERROR_TOPIC_DOC,
                    CONNECTION_GROUP,
                    6,
                    Width.LONG,
                    ERROR_TOPIC_DISPLAY
            )
            .define(
                  ERROR_BATCH_RESPONSE_STATUS_CODE,
                  ConfigDef.Type.STRING,
                  ERROR_BATCH_RESPONSE_STATUS_CODE_DEFAULT,
                  Importance.LOW,
                  ERROR_BATCH_RESPONSE_STATUS_CODE_DOC,
                  CONNECTION_GROUP,
                  7,
                  Width.LONG,
                  ERROR_BATCH_RESPONSE_STATUS_CODE_DISPLAY
            )
            .define(
                  ERROR_BATCH_RESPONSE_KEY,
                  ConfigDef.Type.STRING,
                  ERROR_BATCH_RESPONSE_KEY_DEFAULT,
                  Importance.LOW,
                  ERROR_BATCH_RESPONSE_KEY_DOC,
                  CONNECTION_GROUP,
                  8,
                  Width.LONG,
                  ERROR_BATCH_RESPONSE_KEY_DISPLAY
            )
            .define(
                  ERROR_BATCH_RESPONSE_BODY,
                  ConfigDef.Type.STRING,
                  ERROR_BATCH_RESPONSE_BODY_DEFAULT,
                  Importance.LOW,
                  ERROR_BATCH_RESPONSE_BODY_DOC,
                  CONNECTION_GROUP,
                  9,
                  Width.LONG,
                  ERROR_BATCH_RESPONSE_BODY_DISPLAY
            )
            .define(
                    REGEX_PATTERNS,
                    ConfigDef.Type.STRING,
                    REGEX_PATTERNS_DEFAULT,
                    ConfigDef.Importance.LOW,
                    REGEX_PATTERNS_DOC,
                    REGEX_GROUP,
                    1,
                    ConfigDef.Width.MEDIUM,
                    REGEX_PATTERNS_DISPLAY
            )
            .define(
                    REGEX_REPLACEMENTS,
                    ConfigDef.Type.STRING,
                    REGEX_REPLACEMENTS_DEFAULT,
                    ConfigDef.Importance.LOW,
                    REGEX_REPLACEMENTS_DOC,
                    REGEX_GROUP,
                    2,
                    ConfigDef.Width.MEDIUM,
                    REGEX_REPLACEMENTS_DISPLAY
            )
            .define(
                    REGEX_SEPARATOR,
                    ConfigDef.Type.STRING,
                    REGEX_SEPARATOR_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    REGEX_SEPARATOR_DOC,
                    REGEX_GROUP,
                    3,
                    ConfigDef.Width.SHORT,
                    REGEX_SEPARATOR_DISPLAY
            )
            .define(
                    BATCH_KEY_PATTERN,
                    ConfigDef.Type.STRING,
                    BATCH_KEY_PATTERN_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    BATCH_KEY_PATTERN_DOC,
                    BATCHING_GROUP,
                    0,
                    ConfigDef.Width.SHORT,
                    BATCH_KEY_PATTERN_DISPLAY
            )
            .define(
                    BATCH_MAX_SIZE,
                    ConfigDef.Type.INT,
                    BATCH_MAX_SIZE_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    BATCH_MAX_SIZE_DOC,
                    BATCHING_GROUP,
                    1,
                    ConfigDef.Width.SHORT,
                    BATCH_MAX_SIZE_DISPLAY
            )
            .define(
                    BATCH_PREFIX,
                    ConfigDef.Type.STRING,
                    BATCH_PREFIX_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    BATCH_PREFIX_DOC,
                    BATCHING_GROUP,
                    2,
                    ConfigDef.Width.SHORT,
                    BATCH_PREFIX_DISPLAY
            )
            .define(
                    BATCH_SUFFIX,
                    ConfigDef.Type.STRING,
                    BATCH_SUFFIX_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    BATCH_SUFFIX_DOC,
                    BATCHING_GROUP,
                    3,
                    ConfigDef.Width.SHORT,
                    BATCH_SUFFIX_DISPLAY
            )
            .define(
                    BATCH_SEPARATOR,
                    ConfigDef.Type.STRING,
                    BATCH_SEPARATOR_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    BATCH_SEPARATOR_DOC,
                    BATCHING_GROUP,
                    4,
                    ConfigDef.Width.SHORT,
                    BATCH_SEPARATOR_DISPLAY
            )
            .define(
                   BATCH_BODY_PREFIX,
                   ConfigDef.Type.STRING,
                   BATCH_BODY_PREFIX_DEFAULT,
                   ConfigDef.Importance.HIGH,
                   BATCH_BODY_PREFIX_DOC,
                   BATCHING_GROUP,
                   5,
                   ConfigDef.Width.LONG,
                   BATCH_BODY_PREFIX_DISPLAY
            )
            .define(
                   BATCH_BODY_SUFFIX,
                   ConfigDef.Type.STRING,
                   BATCH_BODY_SUFFIX_DEFAULT,
                   ConfigDef.Importance.HIGH,
                   BATCH_BODY_SUFFIX_DOC,
                   BATCHING_GROUP,
                   6,
                   ConfigDef.Width.LONG,
                   BATCH_BODY_SUFFIX_DISPLAY
            )
            .define(
                    BATCH_BODY_FIELD_FILTER,
                    ConfigDef.Type.LIST,
                    BATCH_BODY_FIELD_FILTER_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    BATCH_BODY_FIELD_FILTER_DOC,
                    BATCHING_GROUP,
                    7,
                    ConfigDef.Width.LONG,
                    BATCH_BODY_FIELD_FILTER_DISPLAY
            )
            .define(
                    SALESFORCE_AUTHENTICATION_ROOT,
                    ConfigDef.Type.STRING,
                    SALESFORCE_AUTHENTICATION_ROOT_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    SALESFORCE_AUTHENTICATION_ROOT_DOC,
                    SALESFORCE_AUTH_GROUP,
                    1,
                    ConfigDef.Width.MEDIUM,
                    SALESFORCE_AUTHENTICATION_ROOT_DISPLAY
            )
            .define(
                    SALESFORCE_AUTHENTICATION_CLIENT_ID,
                    ConfigDef.Type.STRING,
                    SALESFORCE_AUTHENTICATION_CLIENT_ID_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    SALESFORCE_AUTHENTICATION_CLIENT_ID_DOC,
                    SALESFORCE_AUTH_GROUP,
                    2,
                    ConfigDef.Width.MEDIUM,
                    SALESFORCE_AUTHENTICATION_CLIENT_ID_DISPLAY
            )
            .define(
                    SALESFORCE_AUTHENTICATION_USERNAME,
                    ConfigDef.Type.STRING,
                    SALESFORCE_AUTHENTICATION_USERNAME_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    SALESFORCE_AUTHENTICATION_USERNAME_DOC,
                    SALESFORCE_AUTH_GROUP,
                    3,
                    ConfigDef.Width.MEDIUM,
                    SALESFORCE_AUTHENTICATION_USERNAME_DISPLAY
            )
            .define(
                    SALESFORCE_AUTHENTICATION_PRIVATE_KEY,
                    ConfigDef.Type.STRING,
                    SALESFORCE_AUTHENTICATION_PRIVATE_KEY_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    SALESFORCE_AUTHENTICATION_PRIVATE_KEY_DOC,
                    SALESFORCE_AUTH_GROUP,
                    4,
                    ConfigDef.Width.MEDIUM,
                    SALESFORCE_AUTHENTICATION_PRIVATE_KEY_DISPLAY
            )
            .define(
                    KEY_CONVERTER,
                    ConfigDef.Type.STRING,
                    KEY_CONVERTER_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    KEY_CONVERTER_DOC,
                    CONVERTER_GROUP,
                    1,
                    ConfigDef.Width.MEDIUM,
                    KEY_CONVERTER_DISPLAY
            )
            .define(
                    VALUE_CONVERTER,
                    ConfigDef.Type.STRING,
                    VALUE_CONVERTER_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    VALUE_CONVERTER_DOC,
                    CONVERTER_GROUP,
                    2,
                    ConfigDef.Width.MEDIUM,
                    VALUE_CONVERTER_DISPLAY
            )
            .define(
                    VALUE_CONVERTER_SR_URL,
                    ConfigDef.Type.STRING,
                    VALUE_CONVERTER_SR_URL_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    VALUE_CONVERTER_SR_URL_DOC,
                    CONVERTER_GROUP,
                    3,
                    ConfigDef.Width.MEDIUM,
                    VALUE_CONVERTER_SR_URL_DISPLAY
            );

    public final String httpApiUrl;
    public final RequestMethod requestMethod;
    public final int maxRetries;
    public final int retryBackoffMs;
    public String headers;
    public String headerSeparator;
    public String responseTopic;
    public String responseBody;
    public String errorTopic;
    public String errorBatchResponseStatusCode;
    public String errorBatchResponseKey;
    public String errorBatchResponseBody;
    public String regexPatterns;
    public String regexReplacements;
    public String regexSeparator;
    public String batchKeyPattern;
    public String batchPrefix;
    public String batchSuffix;
    public String batchSeparator;
    public String batchBodyPrefix;
    public String batchBodySuffix;
    public List<String> batchBodyFieldFilter;
    public int batchMaxSize;
    public final String salesforceAuthenticationRoot;
    public final String salesforceAuthenticationClientId;
    public final String salesforceAuthenticationUsername;
    public final String salesforceAuthenticationPrivateKey;


    public HttpSinkConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);
        httpApiUrl = getString(HTTP_API_URL);
        maxRetries = getInt(MAX_RETRIES);
        retryBackoffMs = getInt(RETRY_BACKOFF_MS);
        requestMethod = RequestMethod.valueOf(getString(REQUEST_METHOD).toUpperCase());
        headers = getString(HEADERS);
        headerSeparator = getString(HEADER_SEPERATOR);
        responseTopic = getString(RESPONSE_TOPIC);
        responseBody = getString(RESPONSE_BODY);
        errorTopic = getString(ERROR_TOPIC);
        errorBatchResponseStatusCode = getString(ERROR_BATCH_RESPONSE_STATUS_CODE);
        errorBatchResponseKey = getString(ERROR_BATCH_RESPONSE_KEY);
        errorBatchResponseBody = getString(ERROR_BATCH_RESPONSE_BODY);
        regexPatterns = getString(REGEX_PATTERNS);
        regexReplacements = getString(REGEX_REPLACEMENTS);
        regexSeparator = getString(REGEX_SEPARATOR);
        batchKeyPattern = getString(BATCH_KEY_PATTERN);
        batchMaxSize = getInt(BATCH_MAX_SIZE);
        batchPrefix = getString(BATCH_PREFIX);
        batchSuffix = getString(BATCH_SUFFIX);
        batchSeparator = getString(BATCH_SEPARATOR);
        batchBodyPrefix = getString(BATCH_BODY_PREFIX);
        batchBodySuffix = getString(BATCH_BODY_SUFFIX);
        batchBodyFieldFilter = getList(BATCH_BODY_FIELD_FILTER);
        salesforceAuthenticationRoot = getString(SALESFORCE_AUTHENTICATION_ROOT);
        salesforceAuthenticationClientId = getString(SALESFORCE_AUTHENTICATION_CLIENT_ID);
        salesforceAuthenticationUsername = getString(SALESFORCE_AUTHENTICATION_USERNAME);
        salesforceAuthenticationPrivateKey = getString(SALESFORCE_AUTHENTICATION_PRIVATE_KEY);
    }


    private static class EnumValidator implements ConfigDef.Validator {
        private final List<String> canonicalValues;
        private final Set<String> validValues;

        private EnumValidator(List<String> canonicalValues, Set<String> validValues) {
            this.canonicalValues = canonicalValues;
            this.validValues = validValues;
        }

        public static <E> EnumValidator in(E[] enumerators) {
            final List<String> canonicalValues = new ArrayList<>(enumerators.length);
            final Set<String> validValues = new HashSet<>(enumerators.length * 2);
            for (E e : enumerators) {
                canonicalValues.add(e.toString().toLowerCase());
                validValues.add(e.toString().toUpperCase());
                validValues.add(e.toString().toLowerCase());
            }
            return new EnumValidator(canonicalValues, validValues);
        }

        @Override
        public void ensureValid(String key, Object value) {
            if (!validValues.contains(value)) {
                throw new ConfigException(key, value, "Invalid enumerator");
            }
        }

        @Override
        public String toString() {
            return canonicalValues.toString();
        }
    }
}
