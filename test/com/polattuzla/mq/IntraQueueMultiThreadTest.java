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
 * The tests are performed in a single queue that's managed by the QueueManager.
 *
 * It spawns NUM_PAIRS number of producer threads and NUM_PAIRS number of consumer threads.
 *
 * Every thread runs NUM_TRIALS many times and in each run performs:
 * 1) Producer threads:
 *    a) Get a handle to the queue that's being tested
 *    b) Enqueue a message (a byte array object) to the queue
 *
 * 2) Consumer threads:
 *    a) Get a handle to the queue that's being tested
 *    b) Read a message (a byte array object) from the queue
 *    c) Dequeue the previously read message
 *
 * Total number of each type of operations performed are checked for correctness.
 *
 * Performance numbers are reported at the end of the test.
 * A typical output on my computer (Intel Core i7 2.2Ghz, 8 cores) is as follows:
 *
 * >> Performed 2250000 enqueue/read/dequeue operations with 500 threads in 4363 milliseconds
 * >> 515621 operations per second
 *
 * As can be seen, typical number for the total operations is around 500K per second.
 * When only enqueue operations are performed, the total number is 3 times greater. i.e. around 1.5M per second.
 * This is due to the fact that no locking is used in the enqueue operation.
 *
 */
public class IntraQueueMultiThreadTest {
    private static final ExecutorService pool = Executors.newCachedThreadPool();

    /** number of pairs of producer and consumer threads */
    private static final int NUM_PAIRS = 250;

    /** number of enqueu/read/deqeue operations performed per pair of threads */
    private static final int NUM_TRIALS = 3000;


    @Test
    public void test() {
        new MultiThreadTestRunner(NUM_PAIRS, NUM_TRIALS).runTest();
        pool.shutdown();
    }


    class MultiThreadTestRunner {

        private static final String Q_NAME = "Queue-0";

        private static final int TIMEOUT = 60 * 60 * 1000; // in milliseconds

        private final AtomicInteger enqueueSum = new AtomicInteger(0);

        private final AtomicInteger readSum = new AtomicInteger(0);

        private final AtomicInteger dequeueSum = new AtomicInteger(0);

        private final CyclicBarrier barrier;

        private final QueueManager queueManager;

        private final int nTrials, nPairs;

        private final byte[][] byteArrays;

        private final BarrierTimer timer;


        public MultiThreadTestRunner(int npairs, int ntrials) {
            this.nPairs = npairs;
            this.nTrials = ntrials;
            this.queueManager = ConcurrentQueueManager.getInstance();
            this.queueManager.createQueue(Q_NAME);
            this.timer = new BarrierTimer();
            this.barrier = new CyclicBarrier(npairs * 2 + 1, timer);

            int numMax = queueManager.getMaxNumOfBytes();
            byteArrays = new byte[ntrials][numMax];
            for (byte[] b : byteArrays) {
                new Random().nextBytes(b);
            }
        }

        void runTest() {
            try {
                timer.clear();
                for (int i = 0; i < nPairs; i++) {
                    pool.execute((new Producer()));
                    pool.execute((new Consumer()));
                }
                barrier.await(); // wait for all threads to be ready
                barrier.await(); // wait for all threads to finish

                long elapsed = timer.getTime();

                assertEquals("enqueueSum and readSum should be equal", enqueueSum.get(), readSum.get());
                assertEquals("enqueueSum and dequeueSum should be equal", enqueueSum.get(), dequeueSum.get());

                long numOps = nPairs * nTrials * 3; // * 3 for each of enquee/read/dequeue
                System.out.println("Performed " +  numOps + " enqueue/read/dequeue operations with " +
                                    nPairs * 2 + " threads in " + (long) (elapsed / 1e6) + " milliseconds");

                long numOpsPerSec = (long)(numOps / (elapsed / 1e9));
                System.out.println( numOpsPerSec + " operations per second");
                // Sample output:
                //Performed 2250000 enqueue/read/dequeue operations with 500 threads in 4363 milliseconds
                // 515621 operations per second


            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        class Producer implements Runnable {
            public void run() {
                try {
                    barrier.await();
                    for (int i = 0; i < nTrials; i++) {
                        String queueId = queueManager.getQueue(Q_NAME);
                        queueManager.enqueue(queueId, byteArrays[i]);
                        enqueueSum.incrementAndGet();
                    }
                    barrier.await();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
        }

        class Consumer implements Runnable {
            public void run() {
                try {
                    barrier.await();
                    for (int i = 0; i < nTrials; i++) {
                        String queueId = queueManager.getQueue(Q_NAME);
                        ReadResponse r = null;
                        while ((r = queueManager.read(queueId, TIMEOUT)) == null) {
                            Thread.sleep(5);
                        }

                        queueManager.dequeue(queueId, r.getQueueEntityId());
                        readSum.incrementAndGet();
                        dequeueSum.incrementAndGet();
                    }
                    barrier.await();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
        }


    }

}