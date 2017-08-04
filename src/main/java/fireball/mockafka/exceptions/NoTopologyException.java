package fireball.mockafka.exceptions;

public class NoTopologyException extends Exception {

    public NoTopologyException() {
        this("No topology specified. Call topology() on builder.");
    }

    public NoTopologyException(String message) {
        super(message);
    }
}
