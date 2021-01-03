package com.polattuzla.mq.common;

import java.util.Objects;

/*
 * Immutable class representing and Id of a ConcurrentQueue
 */
public class QueueID {
    private final String value;

    public QueueID(String id) {
        this.value = id;
    }

    public QueueID(long id) {
        this.value = String.valueOf(id);
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof QueueID) {
            QueueID another = (QueueID) obj;
            if (this.value.equals(another.value)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.value);
    }

    @Override
    public String toString(){
        return "QueueID: " + this.value;
    }
}