package com.dape.assembler.exceptions;

public class S3OperationException extends RuntimeException {
    public S3OperationException(String message) {
        super(message);
    }
}
