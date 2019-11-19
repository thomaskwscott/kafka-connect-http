package uk.co.threefi.connect.http.sink.dto;

import org.apache.kafka.connect.sink.SinkRecord;

public class RetriableError {

    private String recordKey;
    private String errorMessage;
    private SinkRecord sinkRecord;

    public RetriableError(final SinkRecord sinkRecord, final String errorMessage) {
        this.sinkRecord = sinkRecord;
        this.errorMessage = errorMessage;
    }

    public RetriableError(final String recordKey, final String errorMessage) {
        this.recordKey = recordKey;
        this.errorMessage = errorMessage;
    }

    public String getRecordKey() {
        return recordKey;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public SinkRecord getSinkRecord() {
        return sinkRecord;
    }
}
