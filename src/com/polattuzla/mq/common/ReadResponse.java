package com.polattuzla.mq.common;

import java.util.Arrays;

public class ReadResponse {
    private final String queueId;

    private final String queueEntityId;

    private final byte[] object;

    public ReadResponse(String queueId, String queueEntityId, byte[] object) {
        this.queueId = queueId;
        this.queueEntityId = queueEntityId;
        this.object = object;
    }

    public String getQueueId() {
        return queueId;
    }

    public String getQueueEntityId() {
        return queueEntityId;
    }

    public byte[] getObject() {
        return object;
    }

    @Override
    public String toString() {
        return "ReadResponse{" +
                "queueId='" + queueId + '\'' +
                ", queueEntityId='" + queueEntityId + '\'' +
                ", object=" + Arrays.toString(object) +
                '}';
    }
}
