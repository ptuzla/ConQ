package com.polattuzla.mq;

import com.polattuzla.mq.common.MessageID;
import com.polattuzla.mq.common.QueueID;
import com.polattuzla.mq.common.QueueMessage;
import com.polattuzla.mq.common.ReadResponse;

import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 *  A singleton implementation of QueueManager interface.
 *  Queues are created, contained and managed by this class.
 *  It's the only public interface to clients that use this concurrent queue service.
 *
 */
public class ConcurrentQueueManager implements QueueManager {

    /** A map that maps queue names to queue ids */
    private static ConcurrentHashMap<String, QueueID> qNameToID;

    /** A map that maps queue ids to queues that are managed by this QueueManager */
    private ConcurrentHashMap<QueueID, Queue> queues;

    /** long value that stores id of the next queue to be created */
    private AtomicLong currentId;

    /** singleton instance */
    private static ConcurrentQueueManager instance;

    /** maximum allowed number of bytes to enqueue */
    private static final int MAX_NUM_OF_BYTES = 1000;

    /**
     *  private constructor for ConcurrentQueueManager
     *  It creates and initializes the state variables
     */
    private ConcurrentQueueManager() {
        this.qNameToID = new ConcurrentHashMap<String, QueueID>();
        this.queues = new ConcurrentHashMap<QueueID, Queue>();
        this.currentId = new AtomicLong();
    }

    /**
     * Factory method that returns and instance
     * Instance creation is thread-safe by locking on the class object
     * But it's not expected to be a cause of contention because it's a rare operation
     *
     * @return ConcurrentQueueManager instance that's created
     */

    public static synchronized ConcurrentQueueManager getInstance() {
        if (instance == null) {
            instance = new ConcurrentQueueManager();
        }
        return instance;
    }

    /**
     * Creates a queue with the given name
     *
     * @param queueName
     * @return id of the queue created
     * @throws IllegalStateException if there's already a queue with the same name
     */
    @Override
    public String createQueue(String queueName) throws IllegalStateException {
        checkArgs(queueName);
        QueueID qId = createNewQueue(queueName);
        return qId.getValue();
    }

    /**
     *  Returns the id of the queue for the name given
     *
     * @param queueName
     * @return id of the queue
     * @throws IllegalStateException when there's no queue with the given name
     */
    @Override
    public String getQueue(String queueName) throws IllegalStateException {
        checkArgs(queueName);
        QueueID qId = getQueueIdWithName(queueName);
        return qId.getValue();
    }

    /**
     * Deletes the queue with the given id
     *
     * @param queueId the id of the queue to be deleted
     * @throws IllegalStateException when there's no queue with the given id
     */
    @Override
    public void deleteQueue(String queueId) throws IllegalStateException{
        checkArgs(queueId);
        QueueID qId = getQueueIdWithIdStr(queueId);
        deleteQueueWithId(qId);
    }

    /**
     * Enqueue the object into the queue with the given id.
     *
     * @param queueId id of the queue to which the enqueue operation will be done
     * @param object the byte array that will be enqueued
     */
    @Override
    public void enqueue(String queueId, byte[] object) {
        checkArgs(queueId, object);
        Queue q = getQueueWithId(queueId);
        q.enqueue(object);
    }

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
    @Override
    public ReadResponse read(String queueId, long timeout) throws IllegalArgumentException {
        checkArgs(queueId);
        if (timeout == 0)
            throw new IllegalArgumentException("timeout value cannot be 0");
        Queue q = getQueueWithId(queueId);
        QueueMessage msg = q.read(timeout);
        ReadResponse readResponse = null;

        if (msg != null) {
            MessageID msgId = msg.getMsgID();
            String queueEntityId = msgId.getValue();
            byte[] object = msg.getBytes();
            readResponse = new ReadResponse(queueId, queueEntityId, object);
        }
        return readResponse;
    }

    /**
     * Dequeue the object with the given QueueEntityId.
     * A dequeue permanently removes the object from the queue.
     *
     * @param queueId id of the queue from which the dequeue operation will be done
     * @param queueEntityId id of the message to be dequeued
     * @throws NoSuchElementException if there's no such message to dequeue with the given id
     */
    @Override
    public void dequeue(String queueId, String queueEntityId) throws NoSuchElementException {
        checkArgs(queueId, queueEntityId);
        Queue q = getQueueWithId(queueId);
        MessageID msgId = new MessageID(queueEntityId);
        QueueMessage msg = q.dequeue(msgId);
        if (msg == null) {
            throw new NoSuchElementException("There's no element with the given id to dequeue");
        }
    }

    /**
     * Returns the maximum allowed number of bytes that can be enqueued for a particular QueueManager implementation
     *
     * @return int
     */
    @Override
    public int getMaxNumOfBytes() {
        return MAX_NUM_OF_BYTES;
    }

    /**
     * Method for destroying the ConcurrentQueueManager instance
     *
     */
    public void destroy() {
        this.instance = null;
    }

    /**
     * Internal helper method to create an immutable QueueID object with the id given as String
     *
     * @param idStr
     * @return QueueID
     */
    private QueueID getQueueIdWithIdStr(String idStr) {
        QueueID qId = new QueueID(idStr);
        return qId;
    }

    /**
     * Internal helper method to get the id of queue with the given name
     *
     * @param queueName the name with which to get the queue id
     * @return the queue id with the given name
     * @throws IllegalStateException
     */
    private QueueID getQueueIdWithName(String queueName) throws IllegalStateException {
        QueueID qId = qNameToID.get(queueName);
        if (qId == null) {
            throw new IllegalStateException("A queue with name '" + queueName + "' does not exist");
        }
        return qId;
    }

    /**
     * Internal helper method to get a queue with the given name
     *
     * @param queueName the name with which to get the queue
     * @return the queue with the given name
     * @throws IllegalStateException when a queue with the given name does not exist
     */
    private Queue getQueueWithName(String queueName) throws IllegalStateException {
        QueueID qId = getQueueIdWithName(queueName);
        // Sanity check
        Queue q = queues.get(qId);
        if (q == null) {
            throw new IllegalStateException("A queue with name '" + queueName + "' does not exist");
        }
        return q;
    }

    /**
     * Internal helper method to get a queue with the given id (as QueueID)
     *
     * @param queueId the id with which to get the queue
     * @return the queue with the given name
     * @throws IllegalStateException when a queue with the given id does not exist
     */
    private Queue getQueueWithId(QueueID queueId) throws IllegalStateException {
       Queue q = queues.get(queueId);
        if (q == null) {
            throw new IllegalStateException("A queue with id '" + queueId + "' does not exist");
        }
        return q;
    }

    /**
     * Internal helper method to get a queue with the given id (as String)
     *
     * @param queueId the id with which to get the queue
     * @return the queue with the given name
     * @throws IllegalStateException when a queue with the given id does not exist
     */
    private Queue getQueueWithId(String queueId) throws IllegalStateException {
        QueueID qId = new QueueID(queueId);
        Queue q = getQueueWithId(qId);
        return q;
    }

    /**
     * Internal helper method to be used by the public API methods for null check their arguments
     * Arguments are checked at the beginning of methods to leave the state invariants of the queue manager
     * in a consistent state if a null argument is supplied
     *
     * @param arguments
     */
    private void checkArgs(Object ...arguments) {
        for(Object a:arguments) {
            if (a == null) {
                throw new NullPointerException();
            } else if (a instanceof byte[]) {
                byte[] bytes = (byte[])a;
                if (bytes.length > MAX_NUM_OF_BYTES) {
                    throw new IllegalArgumentException(
                            "Maximum allowed length for argument 'object' is " + MAX_NUM_OF_BYTES);
                }
            }
        }
    }

    /**
     * Internal method to create a new queue with the given queue name
     *
     * We need to use locking mechanism here, because more than one state variables need to be
     * updated atomically, even though they're thread-safe individually.
     *
     * But the lock is in effect on a per queue name basis
     * i.e. Different threads that request different queue names can access simultaneously
     *
     * @param queueName the name with which to create the queue
     * @return the newly created queue with the given name
     * @throws IllegalStateException if there's already a queue with the same name
     */
    private QueueID createNewQueue(String queueName) throws IllegalStateException {

        synchronized (queueName.intern()) {
            QueueID qId = qNameToID.get(queueName);
            if (qId != null) {
                throw new IllegalStateException("A queue with name '" + queueName + "' already exists");
            }

            qId = getNextQueueID();

            QueueID curQId = qNameToID.putIfAbsent(queueName, qId);
            if (curQId != null) {
                qId = curQId;
            }

            Queue newQueue = new ConcurrentThreePhaseQueue(qId, queueName);

            Queue curQueue = queues.putIfAbsent(qId, newQueue);
            if (curQueue != null) {
                newQueue = curQueue;
            }

            return qId;
        }

    }

    /**
     * Internal method to create and store the id for the next queue that's being created
     * @return QueueID for the next queue to be generated
     */
    private QueueID getNextQueueID() {
        long nextId = currentId.getAndIncrement();
        return new QueueID(nextId);
    }

    /**
     * Internal method to delete a queue with the given id (as QueueID)
     *
     * We need to use locking mechanism here, because more than one state variables need to be
     * updated atomically, even though they're thread-safe individually.
     *
     * But the lock is in effect on a per queue name basis
     * i.e. Different threads that request different queue names can access simultaneously
     *
     * @param queueId id of the queue to be deleted
     * @throws IllegalStateException
     */
    private void deleteQueueWithId(QueueID queueId) throws IllegalStateException {
        Queue q = queues.get(queueId);
        if (q == null) {
            throw new IllegalStateException("Queue with id '" + queueId + "' does not exist");
        }
        String queueName = q.getName();

        synchronized (queueName.intern()) {
            q.clear();
            queues.remove(queueId);
            qNameToID.remove(queueName);
        }
    }

}
