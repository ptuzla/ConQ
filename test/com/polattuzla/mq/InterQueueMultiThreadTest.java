package com.polattuzla.mq;

import com.polattuzla.mq.common.ReadResponse;
import com.polattuzla.mq.util.BarrierTimer;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/**
 * This class tests the public API methods that are exposed by the QueueManager under high concurrency.
 * It makes sure that the QueueManager and its underlying mechanics work correctly in a thread-safe manner
 * under high concurrency.
 * The tests are performed in NUM_THREADS many queues 'on a one queue per thread' basis.
 *
 * It spawns NUM_THREADS number of worker threads.
 *
 * Every thread performs:
 * 1- createQueue and getQeueue operations. The queue is exclusive to this thread
 * 2- runs NUM_TRIALS many times the following operations:
 *   a) Enqueue a message (a byte array object) to the queue
 *   b) Read a message (a byte array object) from the queue
 *   c) Dequeue the read message
 * 3- deleteQueue operation
 *
 *
 * Total number of each type of operations performed are checked for correctness.
 *
 * Performance numbers are reported at the end of the test.
 * A typical output on my computer (Intel Core i7 2.2Ghz, 8 cores) is as follows:
 *
 * >> Performed 9000000 createQueue/getQueue/enquee/read/dequeue/deleteQueue operations with 500 threads in 2349 milliseconds
 * >> 3831237 operations per second
 *
 * As can be seen, typical number for the total operations is around 4M per second.
 * It's much higher than the number of intra queue operations that can be performed.
 * This is due to the fact that contention is avoided while every thread is working on its own queue.
 *
 */

public class InterQueueMultiThreadTest {
    private static final ExecutorService pool = Executors.newCachedThreadPool();

    /** number of worker threads */
    private static final int NUM_THREADS = 500;

    /** number of enqueu/read/deqeue operations performed per worker thread */
    private static final int NUM_TRIALS = 3000;


    @Test
    public void test() {
        new MultiThreadTestRunner(NUM_THREADS, NUM_TRIALS).runTest();
        pool.shutdown();
    }


    class MultiThreadTestRunner {

        private static final int TIMEOUT = 60 * 60 * 1000; // in milliseconds

        private final AtomicInteger enqueueSum = new AtomicInteger(0);

        private final AtomicInteger readSum = new AtomicInteger(0);

        private final AtomicInteger dequeueSum = new AtomicInteger(0);

        private final AtomicInteger createQueueSum = new AtomicInteger(0);

        private final AtomicInteger deleteQueueSum = new AtomicInteger(0);

        private final CyclicBarrier barrier;

        private final QueueManager queueManager;

        private final int nTrials, nWorkers;

        private final byte[][] byteArrays;

        private final BarrierTimer timer;


        public MultiThreadTestRunner(int nworkers, int ntrials) {
            this.nWorkers = nworkers;
            this.nTrials = ntrials;
            this.queueManager = ConcurrentQueueManager.getInstance();
            this.timer = new BarrierTimer();
            this.barrier = new CyclicBarrier(nworkers + 1, timer);

            int numMax = queueManager.getMaxNumOfBytes();
            byteArrays = new byte[ntrials][numMax];
            for (byte[] b : byteArrays) {
                new Random().nextBytes(b);
            }
        }

        void runTest() {
            try {
                timer.clear();
                for (int i = 0; i < nWorkers; i++) {
                    pool.execute((new Worker()));
                }
                barrier.await(); // wait for all threads to be ready
                barrier.await(); // wait for all threads to finish

                long elapsed = timer.getTime();

                assertEquals("createQueueSum and deleteQueueSum should be equal", createQueueSum.get(), deleteQueueSum.get());
                assertEquals("enqueueSum and readSum should be equal", enqueueSum.get(), readSum.get());
                assertEquals("enqueueSum and dequeueSum should be equal", enqueueSum.get(), dequeueSum.get());

                // * 6 for each of createQueue/getQueue/enquee/read/dequeue/deleteQueue
                long numOps = nWorkers * nTrials * 6;
                System.out.println("Performed " +  numOps +
                        " createQueue/getQueue/enquee/read/dequeue/deleteQueue operations with " +
                        nWorkers + " threads in " + (long) (elapsed / 1e6) + " milliseconds");

                long numOpsPerSec = (long)(numOps / (elapsed / 1e9));
                System.out.println( numOpsPerSec + " operations per second");
                // Sample output:
                // Performed 9000000 createQueue/getQueue/enquee/read/dequeue/deleteQueue operations with 500 threads in 2349 milliseconds
                // 3831237 operations per second


            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        class Worker implements Runnable {

            public void run() {
                try {
                    barrier.await();
                    String qName = "Queue_" + Thread.currentThread().getId();
                    String queueId = queueManager.createQueue(qName);
                    createQueueSum.incrementAndGet();
                    String queueId2 = queueManager.getQueue(qName);
                    assertEquals(queueId, queueId2);

                    for (int i = 0; i < nTrials; i++) {
                        queueManager.enqueue(queueId, byteArrays[i]);
                        enqueueSum.incrementAndGet();
                        ReadResponse r = null;
                        while ((r = queueManager.read(queueId, TIMEOUT)) == null) {
                            //Thread.sleep(5);
                        }
                        queueManager.dequeue(queueId, r.getQueueEntityId());
                        readSum.incrementAndGet();
                        dequeueSum.incrementAndGet();
                    }

                    queueManager.deleteQueue(queueId);
                    deleteQueueSum.incrementAndGet();

                    barrier.await();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
        }

    }

}