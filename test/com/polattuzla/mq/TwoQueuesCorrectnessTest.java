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
 * It performs these tests using two queues that's created via QueueManager.
 *
 */
public class TwoQueuesCorrectnessTest extends TestSetup {

    @Test
    public void testAPIMethods() throws InterruptedException {
        QueueManager queueManager = init();

        int timeout = 3000;

        // Create a queue
        String queueId0 = queueManager.createQueue("Queue-0");

        // Get the queue id with the same name
        String queueId1_2 = queueManager.getQueue("Queue-0");

        // check that getQueue returns the correct id
        assertEquals(queueId0, queueId1_2);

        // Create a queue
        String queueId1 = queueManager.createQueue("Queue-1");

        // Get the queue id with the same name
        String queueId2_2 = queueManager.getQueue("Queue-1");

        // check that getQueue returns the correct id
        assertEquals(queueId1, queueId2_2);


        // Enqueue three randomly initialized byte arrays to queues 0 and 1 in random order
        queueManager.enqueue(queueId0, byteArrays[0]);
        queueManager.enqueue(queueId0, byteArrays[1]);
        queueManager.enqueue(queueId1, byteArrays[10]);
        queueManager.enqueue(queueId1, byteArrays[11]);
        queueManager.enqueue(queueId1, byteArrays[12]);
        queueManager.enqueue(queueId0, byteArrays[2]);

        // Read them from the queues with a timeout value of 'timeout' defined above
        ReadResponse r0_0 = queueManager.read(queueId0, timeout);
        ReadResponse r1_0 = queueManager.read(queueId1, timeout);

        ReadResponse r0_1 = queueManager.read(queueId0, timeout);
        ReadResponse r1_1 = queueManager.read(queueId1, timeout);

        ReadResponse r0_2 = queueManager.read(queueId0, timeout);
        ReadResponse r1_2 = queueManager.read(queueId1, timeout);


        // Check that the byte arrays received in the ReadResponse objects are correct
        assertTrue(Arrays.equals(byteArrays[0], r0_0.getObject()));
        assertTrue(Arrays.equals(byteArrays[1], r0_1.getObject()));
        assertTrue(Arrays.equals(byteArrays[2], r0_2.getObject()));

        assertTrue(Arrays.equals(byteArrays[10], r1_0.getObject()));
        assertTrue(Arrays.equals(byteArrays[11], r1_1.getObject()));
        assertTrue(Arrays.equals(byteArrays[12], r1_2.getObject()));

        // Check that when the queue is empty, any further read requests return null as a ReadResponse
        ReadResponse rNull0 = queueManager.read(queueId0, timeout);
        assertNull(rNull0);

        ReadResponse rNull1 = queueManager.read(queueId1, timeout);
        assertNull(rNull1);

        // Dequeue the first two of the messages (byte arrays) from the queue
        // Check that they do not throw any exceptions
        try {
            queueManager.dequeue(queueId0, r0_0.getQueueEntityId());
            queueManager.dequeue(queueId0, r0_1.getQueueEntityId());

            queueManager.dequeue(queueId1, r1_0.getQueueEntityId());
            queueManager.dequeue(queueId1, r1_1.getQueueEntityId());

        } catch (Exception e) {
            assertTrue("dequeue operations should work until timeout", false);
        }


        // Wait for the duration of the timeout to make sure that the third read message gets a timeout
        Thread.sleep(timeout);
        // Try to dequeue the third message from the queues
        // Check that it does throw NoSuchElementException
        try {
            queueManager.dequeue(queueId0, r0_2.getQueueEntityId());
            assertTrue("dequeue operation should not work after the timeout", false);
        } catch (NoSuchElementException e) {
        }
        try {
            queueManager.dequeue(queueId1, r1_2.getQueueEntityId());
            assertTrue("dequeue operation should not work after the timeout", false);
        } catch (NoSuchElementException e) {
        }

        r0_2 = queueManager.read(queueId0, timeout);
        assertTrue(Arrays.equals(byteArrays[2], r0_2.getObject()));

        r1_2 = queueManager.read(queueId1, timeout);
        assertTrue(Arrays.equals(byteArrays[12], r1_2.getObject()));


        // Try to dequeue the third message from the queues
        // Check that it does not throw NoSuchElementException
        try {
            queueManager.dequeue(queueId0, r0_2.getQueueEntityId());
            queueManager.dequeue(queueId1, r1_2.getQueueEntityId());

        } catch (NoSuchElementException e) {
            assertTrue("dequeue operation should be possible at this stage", false);
        }

        // Check that another dequeue with the same message id after a successful one throws an Exception
        try {
            queueManager.dequeue(queueId0, r0_2.getQueueEntityId());
            assertTrue("dequeue operation should not work after a successful dequeue", false);
        } catch (NoSuchElementException e) {
        }
        try {
            queueManager.dequeue(queueId1, r1_2.getQueueEntityId());
            assertTrue("dequeue operation should not work after a successful dequeue", false);
        } catch (NoSuchElementException e) {
        }

        // Delete the queues
        // Check that it does not throw exception
        try {
            queueManager.deleteQueue(queueId0);
            queueManager.deleteQueue(queueId1);
        } catch (IllegalStateException e) {
            assertTrue("deleteQueue operation should be possible at this stage", false);
        }
        // Check that another dequeue with the same message id after a successful one throws an Exception
        try {
            queueManager.deleteQueue(queueId0);
            assertTrue("deleteQueue operation should throw an exception after a successful deletQueue", false);
        } catch (IllegalStateException e) {
        }

        // Check that another dequeue with the same message id after a successful one throws an Exception
        try {
            queueManager.deleteQueue(queueId1);
            assertTrue("deleteQueue operation should throw an exception after a successful deletQueue", false);
        } catch (IllegalStateException e) {
        }

        destroyQueueManager();
    }
}
