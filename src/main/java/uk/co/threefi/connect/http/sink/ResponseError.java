package uk.co.threefi.connect.http.sink;

import org.apache.kafka.connect.sink.SinkRecord;

public class ResponseError {

    private String recordKey;
    private String errorMessage;
    private SinkRecord sinkRecord;

    public ResponseError(SinkRecord sinkRecord, String errorMessage) {
        this.sinkRecord = sinkRecord;
        this.errorMessage = errorMessage;
    }

    public ResponseError(String recordKey, String errorMessage) {
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
