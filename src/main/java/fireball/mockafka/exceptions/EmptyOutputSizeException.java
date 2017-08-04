package fireball.mockafka.exceptions;

public class EmptyOutputSizeException extends Exception {

    public EmptyOutputSizeException() {
        this("Output size needs to be greater than 0.");
    }

    public EmptyOutputSizeException(String message) {
        super(message);
    }
}
