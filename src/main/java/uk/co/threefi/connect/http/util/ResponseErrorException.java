package uk.co.threefi.connect.http.util;

public class ResponseErrorException extends Exception {
    static final long serialVersionUID = -7387517993174229748L;

    public ResponseErrorException(final String message) {
        super(message);
    }
}
