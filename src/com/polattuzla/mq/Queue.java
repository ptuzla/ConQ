package com.polattuzla.mq;


import com.polattuzla.mq.common.MessageID;
import com.polattuzla.mq.common.QueueID;
import com.polattuzla.mq.common.QueueMessage;

import java.util.NoSuchElementException;

public interface  Queue {

    public QueueID getId();

    public String getName();

    public void enqueue(byte[] object);

    public QueueMessage read(long timeout);

    public QueueMessage dequeue(MessageID messageId);

    public void clear();

}
