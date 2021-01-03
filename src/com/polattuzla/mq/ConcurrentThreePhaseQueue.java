package com.polattuzla.mq;

import com.polattuzla.mq.common.MessageID;
import com.polattuzla.mq.common.QueueID;
import com.polattuzla.mq.common.QueueMessage;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A queue abstraction that's made of four concurrent data structures.
 * 1) ConcurrentLinkedQueue : phase1 queue
 * 2) ConcurrentHashMap     : phase2 table
 * 3) ConcurentSkipListMap  : a time table for phase2
 * 4) ConcurrentLinkedQueue : phase3 queue
 *
 * Mechanics:
 * The lifecycle of a message in ConcurrentThreePhaseQueue (abbr. CTPQ) includes three phases and their corresponding
 * data structures
 *
 * phase1: A message enqueued to CTPQ is in phase1. It's enqueued at the tail of an internal queue named phase1
 *         which is an instance of ConcurrentLinkedQueue
 *
 * phase2: When a message is read from phase1 queue, it's placed in a map called phase2, which is a ConcurrentHashMap.
 *         The message is now in phase2. During phase2 the messages have to be ordered according to the time they will
 *         timeout. So their headers are placed in a heap like data structure which is a ConcurrentSkipListMap. The keys
 *         to these headers consist of strings which are formed by numerically adding read time stamp and timeout of the
 *         message. This ensures that message headers are ordered according to their order in which they timeout after
 *         a read operation.  To ensure uniqueness of keys, message id is also appended to the key. The (now string) keys
 *         are lexicographically ordered in this manner.
 *         At this stage the message may be removed from phase2, which is the dequeuing of the message from CTPQ actually.
 *
 * phase3: If the timeout duration which is supplied by the read call that made the transition from phase1 to phase2
 *         is passed, the message is enqueued to the phase3 queue, which is another ConcurrentLinkedQueue instance.
 *         The message is now in phase3.
 *
 *
 * Messages are enqueued to phase3 queue in the order based on their timeout ages.
 * i.e. The head of the phase3 queue has the message whose timeout has passed least recently, and it's to be read by
 * the subsequent read calls to CTPQ first among the all messages in the phase1 queue and phase3 queue.
 *
 * The tail of the phase3 queue has the message whose timeout has passed most recently, and it's to be read by
 * the subsequent read calls to CTPQ last among the all messages in the phase3 queue.
 *
 * All read calls to CTPQ read from phase3 first, to give the messages in phase3 precedence over those in phase1
 * Because they have already been at the head of the phase1 queue once, read, and placed in phase2 and left there
 * due to failure to dequeue them at the client side.
 *
 * When the messages in phase3 consumed, subsequent read calls to CTPQ read from phase1 queue as long as the phase3
 * queue is empty.
 *
 * Maintenance:
 * Before every read and dequeue operation, maintenance work takes place. This makes sure that if a message
 * in phase2 times out after a read, it's passed to phase3 and is no longer available for a dequeue but a read.
 *
 * Concurrency:
 * The four concurrent data structures that make up this queue abstraction have optimistic concurrency semantics.
 * Their implementation in the Java collections library avoid using synchronization and utilize compare and swap
 * functions (CAS) and algorithms. So whenever possible locking is avoided in the implementation of this class.
 * This is so for the implementation of enqueue operation. However other api methods have to update multiple concurrent
 * data structures atomically in order to keep them in a consistent state and use explicit locking mechanisms.
 * So this gives this ConcurrentThreePhaseQueue class a much higher concurrency in the enqueue operation than
 * the read and dequeue operations which have synchronized semantics.
 *
 * Iterators of the underlying data structures are weakly consistent, returning elements reflecting the state of the
 * queue or map at some point at or since the creation of the iterator. They do not guarantee consistent updates to and
 * fresh reads from the data structure. So in the implementation of CTPQ, their use is avoided.
 *
 * Time complexity:
 * Enqueue operation: O(1)
 * Read operation: O(1) if no maintenance necessary. O(log(n)) if maintenance is performed.
 * Dequeue operation: O(log(n))
 *
 */
public class ConcurrentThreePhaseQueue implements Queue {

    private final QueueID id;

    private final String name;

    /** long value that stores id of the next message to be created */
    private AtomicLong currentId;

    /** phase1 queue for enqueue operations and read operations */
    private final ConcurrentLinkedQueue<QueueMessage> phase1;

    /** phase2 heap for read objects until timeout */
    private final ConcurrentHashMap<MessageID, QueueMessage> phase2;

    /** phase3 queue for read operations of message with timeout */
    private final ConcurrentLinkedQueue<QueueMessage> phase3;

    /** mapping of times when messages timeout to message headers */
    private final ConcurrentSkipListMap<String, MessageID> timeTable;

    /**
     *  Constructor for ConcurrentThreePhaseQueue
     *  It creates and initializes the state variables
     */
    public ConcurrentThreePhaseQueue(QueueID id, String name) {
        this.id = id;
        this.name = name;
        this.phase1 = new ConcurrentLinkedQueue<QueueMessage>();
        this.phase2 = new ConcurrentHashMap<MessageID, QueueMessage>();
        this.phase3 = new ConcurrentLinkedQueue<QueueMessage>();
        this.timeTable = new ConcurrentSkipListMap<String, MessageID>();
        this.currentId = new AtomicLong();
    }

    @Override
    public QueueID getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * Inserts the specified element at the tail of this queue.
     * As the phase1 queue is unbounded, this add method will never throw
     * IllegalStateException or return false.
     * So return type is void
     *
     * @throws NullPointerException if the specified element is null
     */
    @Override
    public void enqueue(byte[] object) {
        MessageID msgId = getNextMessageID();
        QueueMessage msg = new QueueMessage(object, msgId);
        boolean res = phase1.add(msg);
    }

    /**
     * Reads a message from CTPQ.
     * Before every read operation maintenance takes place if necessary as described in the class documenation above.
     * First phase3 queue is checked and if not found any, phase1 queue is checked for a message to read.
     *
     *
     * @param timeout duration, during which the message can be dequeued from CTPQ. After that duration
     *                the message has to be read again, before it's tried to be dequeued
     * @return QueueMessage, the message read from the CTPQ. null if the queues are empty
     */
    @Override
    public  QueueMessage read(long timeout) {
        maintainPhase2AndPhase3();
        synchronized (this) {
            long readTimeStamp = System.currentTimeMillis();
            QueueMessage msg = phase3.poll();
            if (msg != null) {
                QueueMessage newMsg = new QueueMessage(msg, timeout, readTimeStamp);
                putToPhase2(newMsg);
            } else {
                msg = phase1.poll();
                if (msg != null) {
                    QueueMessage newMsg = new QueueMessage(msg, timeout, readTimeStamp);
                    putToPhase2(newMsg);
                }
            }
            return msg;
        }
    }

    /**
     * Dequeues a message from CTPQ
     * Before every read operation maintenance takes place if necessary as described in the class documenation above.
     *
     * @param messageId with which to dequeue message
     * @return QueueMessage, the message dequeued from the CTPQ. null if the queues are empty
     */
    @Override
    public QueueMessage dequeue(MessageID messageId) {
        maintainPhase2AndPhase3();
        synchronized (this) {
            QueueMessage msg = phase2.remove(messageId);
            if (msg != null) { // message may have been timeout and moved from phase2 to phase3. So null check
                timeTable.remove(msg.getTimeTableId());
            }
            return msg;
        }
    }

    /**
     * Remove all messages in the queue
     */
    @Override
    public void clear() {
        phase1.clear();
        phase2.clear();
        phase3.clear();
    }

    /**
     * Internal method that maintains the states of phase2 and phase3 for messages that have stayed in phase2 longer
     * than their respective timeout value
     *
     */
    private void maintainPhase2AndPhase3() {
        synchronized (this) {
            QueueMessage msg = null;
            MessageID msgId = null;
            String tId = null;
            Map.Entry<String, MessageID> entry = null;

            while ((entry = timeTable.firstEntry()) != null) {
                tId = entry.getKey();
                msgId = entry.getValue();
                msg = phase2.get(msgId);
                boolean to = hasTimedOut(msg);
                if (!to) {
                    break;
                }
                msg = removeFromPhase2(tId, msgId);
                phase3.add(msg);
            }
        }
    }

    /**
     * For every entry that's inserted into phase2 table
     * makes a mapping in the timetable in the form: "1518658454610_MessageID: 0" -> "MessageID: 0",
     *                                               "1518658455614_MessageID: 1" -> "MessageID: 1"  etc.
     *
     * @param msg to be inserted in phase2 table
     */
    private void putToPhase2(QueueMessage msg) {
        MessageID msgId = msg.getMsgID();
        Long readTimeStamp = msg.getReadTimeStamp();
        String tId = msg.getTimeTableId();
        timeTable.put(tId, msgId);

        phase2.put(msg.getMsgID(), msg);
    }

    /**
     * Removes a message from phase2 map and from time table at the same time.
     *
     * @param tId    timeTable id of the message
     * @param msgId  message id of the message
     * @return  QueueMessage that's removed from phase 2
     */
    private QueueMessage removeFromPhase2(String tId, MessageID msgId) {
        MessageID other = timeTable.remove(tId);
        //Sanity check
        assert(other == msgId);
        QueueMessage msg = phase2.remove(msgId);
        return msg;
    }

    /**
     * Internal method that checks whether a particular message has stayed in phase2 longer than its respective
     * timeout value
     *
     * @param message for which to check for timeout
     * @return boolean result for the test
     */
    private boolean hasTimedOut(QueueMessage message) {
        long now = System.currentTimeMillis();
        if (now - message.getReadTimeStamp() > message.getTimeout())
            return true;
        return false;
    }

    /**
     * Internal method to create and store the id for the next message id that's being created
     *
     * @return MessageID for the next queue to be generated
     */
    private MessageID getNextMessageID() {
        long nextId = currentId.getAndIncrement();
        return new MessageID(nextId);
    }
}