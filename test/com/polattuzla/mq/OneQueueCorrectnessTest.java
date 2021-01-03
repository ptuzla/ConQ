package com.polattuzla.mq;

import com.polattuzla.mq.common.ReadResponse;
import org.junit.Test;

import java.util.Arrays;
import java.util.NoSuchElementException;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * This test class tests for correctness every API method that QueueManager exposes.
 * It performs these tests using a single queue that's created via QueueManager.
 *
 */
public class OneQueueCorrectnessTest extends TestSetup {

    @Test
    public void testAPIMethods() throws InterruptedException {
        QueueManager queueManager = init();

        int timeout = 3000;

        // Create a queue
        String queueId0 = queueManager.createQueue("Queue-0");

        // Get the queue id with the same name
        String queueIdRcvd = queueManager.getQueue("Queue-0");

        // check that getQueue returns the correct id
        assertEquals(queueId0, queueIdRcvd);

        // Enqueue three randomly initialized byte arrays
        queueManager.enqueue(queueId0, byteArrays[0]);
        queueManager.enqueue(queueId0, byteArrays[1]);
        queueManager.enqueue(queueId0, byteArrays[2]);

        // Read them from the queue with a timeout value of 'timeout' defined above
        ReadResponse r0 = queueManager.read(queueId0, timeout);
        ReadResponse r1 = queueManager.read(queueId0, timeout);
        ReadResponse r2 = queueManager.read(queueId0, timeout);

        // Check that the byte arrays received in the ReadResponse objects are correct
        assertTrue(Arrays.equals(byteArrays[0], r0.getObject()));
        assertTrue(Arrays.equals(byteArrays[1], r1.getObject()));
        assertTrue(Arrays.equals(byteArrays[2], r2.getObject()));

        // Check that when the queue is empty, any further read requests return null as a ReadResponse
        ReadResponse rNull = queueManager.read(queueId0, timeout);
        assertNull(rNull);

        // A timeout argument of 0 in a read call should raise an exception
        try {
            rNull = queueManager.read(queueId0, 0);
            assertTrue("A read with a 0 timeout should raise IllegalArgumentException", false);
        } catch (IllegalArgumentException e) {
        }

        // Dequeue the first two of the messages (byte arrays) from the queue
        // Check that they do not throw any exceptions
        try {
            queueManager.dequeue(queueId0, r0.getQueueEntityId());
            queueManager.dequeue(queueId0, r1.getQueueEntityId());
        } catch (Exception e) {
            assertTrue("dequeue operations should work until timeout", false);
        }


        // Wait for the duration of the timeout to make sure that the third read message gets a timeout
        Thread.sleep(timeout);
        // Try to dequeue the third message from the queue
        // Check that it does throw NoSuchElementException
        try {
            queueManager.dequeue(queueId0, r2.getQueueEntityId());
            assertTrue("dequeue operation should not work after the timeout", false);
        } catch (NoSuchElementException e) {
        }

        r2 = queueManager.read(queueId0, timeout);
        assertTrue(Arrays.equals(byteArrays[2], r2.getObject()));


        // Try to dequeue the third message from the queue
        // Check that it does not throw NoSuchElementException
        try {
            queueManager.dequeue(queueId0, r2.getQueueEntityId());

        } catch (NoSuchElementException e) {
            assertTrue("dequeue operation should be possible at this stage", false);
        }

        // Check that another dequeue with the same message id after a successful one throws an Exception
        try {
            queueManager.dequeue(queueId0, r2.getQueueEntityId());
            assertTrue("dequeue operation should throw an exception after a successful dequeue", false);
        } catch (NoSuchElementException e) {
        }

        // Delete the queue
        // Check that it does not throw exception
        try {
            queueManager.deleteQueue(queueId0);
        } catch (IllegalStateException e) {
            assertTrue("deleteQueue operation should be possible at this stage", false);
        }
        // Check that another dequeue with the same message id after a successful one throws an Exception
        try {
            queueManager.deleteQueue(queueId0);
            assertTrue("deleteQueue operation should throw an exception after a successful deletQueue", false);
        } catch (IllegalStateException e) {
        }

        destroyQueueManager();
    }
}
