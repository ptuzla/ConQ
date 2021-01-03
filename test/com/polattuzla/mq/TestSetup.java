package com.polattuzla.mq;

import com.polattuzla.mq.common.ReadResponse;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

public class TestSetup {

    protected byte[][] byteArrays;

    protected QueueManager getQueueManager() {
        return ConcurrentQueueManager.getInstance();
    }

    protected void destroyQueueManager() {
        ConcurrentQueueManager.getInstance().destroy();
    }

    protected void initializeByteArrays() {
        int numMax = getQueueManager().getMaxNumOfBytes();
        byteArrays = new byte[100][numMax];
        for (byte[] b: byteArrays) {
            new Random().nextBytes(b);
        }
    }

    protected QueueManager init() {
        QueueManager queueManager = getQueueManager();
        initializeByteArrays();
        return queueManager;
    }

    protected void destroy() {
        destroyQueueManager();
    }

}
