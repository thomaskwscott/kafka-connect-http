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

package uk.co.threefi.connect.http.sink;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
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

    public static final String BATCH_SEPARATOR = "batch.seperator";
    private static final String BATCH_SEPARATOR_DEFAULT = ",";
    private static final String BATCH_SEPARATOR_DOC = "seperator for records in a batch.";
    private static final String BATCH_SEPARATOR_DISPLAY = "Batch seperator";

    public static final String BATCH_LINGER_MS = "batch.linger.ms";
    private static final Long BATCH_LINGER_MS_DEFAULT = 0l;
    private static final String BATCH_LINGER_MS_DOC = "interval to wait to accumulate records in a batch. "
            + "After this interval has passed the batch will be sent whether full or not. "
            + "Note: this relies on Connect's flush mechanism so should not be less than offset.flush.interval.ms";
    private static final String BATCH_LINGER_MS_DISPLAY = "Batch linger ms";

    public static final String CONNECTION_GROUP = "Connection";
    private static final String RETRIES_GROUP = "Retries";
    private static final String REGEX_GROUP = "Regex";
    private static final String BATCHING_GROUP = "Batching";

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
                    BATCH_LINGER_MS,
                    ConfigDef.Type.LONG,
                    BATCH_LINGER_MS_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    BATCH_LINGER_MS_DOC,
                    BATCHING_GROUP,
                    5,
                    ConfigDef.Width.SHORT,
                    BATCH_LINGER_MS_DISPLAY
            );

    public final String httpApiUrl;
    public final RequestMethod requestMethod;
    public final int maxRetries;
    public final int retryBackoffMs;
    public String headers;
    public String headerSeparator;
    public String regexPatterns;
    public String regexReplacements;
    public String regexSeparator;
    public String batchKeyPattern;
    public String batchPrefix;
    public String batchSuffix;
    public String batchSeparator;
    public Long batchLingerMs = BATCH_LINGER_MS_DEFAULT;
    public int batchMaxSize;


    public HttpSinkConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);
        httpApiUrl = getString(HTTP_API_URL);
        maxRetries = getInt(MAX_RETRIES);
        retryBackoffMs = getInt(RETRY_BACKOFF_MS);
        requestMethod = RequestMethod.valueOf(getString(REQUEST_METHOD).toUpperCase());
        headers = getString(HEADERS);
        headerSeparator = getString(HEADER_SEPERATOR);
        regexPatterns = getString(REGEX_PATTERNS);
        regexReplacements = getString(REGEX_REPLACEMENTS);
        regexSeparator = getString(REGEX_SEPARATOR);
        batchKeyPattern = getString(BATCH_KEY_PATTERN);
        batchMaxSize = getInt(BATCH_MAX_SIZE);
        batchPrefix = getString(BATCH_PREFIX);
        batchSuffix = getString(BATCH_SUFFIX);
        batchSeparator = getString(BATCH_SEPARATOR);
        batchLingerMs = getLong(BATCH_LINGER_MS);
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

    public static void main(String... args) {
        System.out.println(CONFIG_DEF.toEnrichedRst());
    }
}
