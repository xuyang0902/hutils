package com.ebuer.exception;

/**
 * @author xu.qiang
 * @date 2017/1/9.
 */
public class HbaseComponentException extends RuntimeException {

    public HbaseComponentException() {
    }

    public HbaseComponentException(String message) {
        super(message);
    }

    public HbaseComponentException(String message, Throwable cause) {
        super(message, cause);
    }
}
