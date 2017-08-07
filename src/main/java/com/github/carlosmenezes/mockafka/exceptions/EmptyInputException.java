package com.github.carlosmenezes.mockafka.exceptions;

public class EmptyInputException extends Exception {

    public EmptyInputException() {
        this("No input fixtures specified. Call input() method on builder.");
    }

    public EmptyInputException(String message) {
        super(message);
    }
}
