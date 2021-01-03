package com.polattuzla.mq.common;

import java.util.Objects;

/*
 * Immutable class representing and Id of a QueueMessage
 */
public class MessageID {
    private final String value;

    public MessageID(String value) {
        this.value = value;
    }

    public MessageID(long id) {
        this.value = String.valueOf(id);
    }

    public String getValue() {
        return value;
    }


    @Override
    public boolean equals(Object obj) {
        if (obj instanceof MessageID) {
            MessageID another = (MessageID) obj;
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
        return "MessageID: " + this.value;
    }

//    @Override
//    public int compareTo(MessageID other) {
//        return value.compareTo(other.getValue());
//    }
}
