package com.polattuzla.mq;

import com.polattuzla.mq.common.ReadResponse;

import java.util.NoSuchElementException;

public interface QueueManager {

    /**
     * Creates a queue with the given name
     *
     * @param queueName
     * @return id of the queue created
     * @throws IllegalStateException
     */
    public String createQueue(String queueName) throws IllegalStateException;

    /**
     *   Returns the id of the queue for the name given
     *
     * @param queueName
     * @return id of the queue
     * @throws IllegalStateException when there's no queue with the given name
     */
    public String getQueue(String queueName) throws IllegalStateException;

    /**
     * Deletes the queue with the given id
     *
     * @param queueId the id of the queue to be deleted
     * @throws IllegalStateException when there's no queue with the given id
     */
    public void deleteQueue(String queueId) throws IllegalStateException;

    /**
     * Enqueue the object into the queue with the given id.
     *
     * @param queueId id of the queue to which the enqueue operation will be done
     * @param object the byte array that will be enqueued
     */
    public void enqueue(String queueId, byte[] object);

    /**
     * Read an object from the queue.
     * This object will not be returned as a response to another read until the given timeout has expired.
     * Once dequeue is called then the object will not be returned by a subsequent read.
     *
     * @param queueId id of the queue from which the read operation will be done
     * @param timeout timeout duration in milliseconds
     * @return ReadResponse object that represent the message read
     * @throws IllegalArgumentException if timeout argument is 0
     */
    public ReadResponse read(String queueId, long timeout) throws IllegalArgumentException;

    /**
     * Dequeue the object with the given QueueEntityId.
     * A dequeue permanently removes the object from the queue.
     *
     * @param queueId id of the queue from which the dequeue operation will be done
     * @param queueEntityId id of the message to be dequeued
     * @throws NoSuchElementException if there's no such message to dequeue with the given id
     */
    public void dequeue(String queueId, String queueEntityId) throws NoSuchElementException;

    /**
     * Returns the maximum allowed number of bytes that can be enqueued for a particular QueueManager implementation
     *
     * @return length of byte array that can be enqueued
     */
    public int getMaxNumOfBytes();


}