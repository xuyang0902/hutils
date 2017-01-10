package com.ebuer.exception;

/**
 * @author xu.qiang
 * @date 2017/1/9.
 */
public class PhoenixException extends RuntimeException {

    public PhoenixException() {
    }

    public PhoenixException(String message) {
        super(message);
    }

    public PhoenixException(String message, Throwable cause) {
        super(message, cause);
    }
}
