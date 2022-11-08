package course.kafka.exception;

public class JsonSerializationException extends RuntimeException {
    public JsonSerializationException() {
    }

    public JsonSerializationException(String message) {
        super(message);
    }

    public JsonSerializationException(String message, Throwable cause) {
        super(message, cause);
    }

    public JsonSerializationException(Throwable cause) {
        super(cause);
    }
}
