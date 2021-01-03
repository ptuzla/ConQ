package com.polattuzla.mq.common;

import java.util.Arrays;

/*
    Immutable class representing an object in a ConcurrentQueue
 */
public class QueueMessage {

    private final byte[] bytes;

    private final MessageID msgID;

    private final long timeout;

    private final long readTimeStamp;

    private final String timeTableId;

    /**
     * Constructor that makes a copy of the bytes for being immutable and hence thread-safe
     *
     * @param bytes
     * @param msgID
     */

    public QueueMessage(byte[] bytes, MessageID msgID) {
        this.bytes = Arrays.copyOf(bytes, bytes.length);
        this.msgID = msgID;
        this.timeout = 0;
        this.readTimeStamp = 0;
        this.timeTableId = null;
    }

    /**
     * Copy constructor
     * Makes a copy of the bytes for being immutable and hence thread-safe
     * Initializes the readTimeStamp with the current time
     *
     * @param other another instance of QeueuMessage
     */
    public QueueMessage(QueueMessage other, long timeout, long readTimeStamp) {
        this.bytes = Arrays.copyOf(other.bytes, other.bytes.length);
        this.msgID = other.msgID;
        this.timeout = timeout;
        this.readTimeStamp = readTimeStamp;
        this.timeTableId = makeTimeTableId();
    }

    /**
     * Returns a copy of the bytes for being immutable and hence thread-safe
     * @return
     */
    public byte[] getBytes() {
        if (bytes == null) {
            return null;
        } else {
            return Arrays.copyOf(this.bytes, bytes.length);
        }
    }

    /**
     * Getter for msgID
     * @return msgID
     */
    public MessageID getMsgID() {
        return this.msgID;
    }


    /**
     * Getter for timeout
     * @return timeout
     */
    public long getTimeout() {
        return timeout;
    }

    /**
     * Getter for readTimeStamp
     * @return readTimeStamp
     */
    public long getReadTimeStamp() {
        return readTimeStamp;
    }

    /**
     * Getter for timeTableId
     * @return timeTableId
     */
    public String getTimeTableId() {
        return timeTableId;
    }

    private String makeTimeTableId() {
        return (long)(readTimeStamp + timeout) + "_" + msgID;
    }
}