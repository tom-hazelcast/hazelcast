package com.hazelcast.raft.service.atomiclong;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.raft.service.spi.RaftProxyFactory;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.InjectTestContext;
import com.hazelcast.simulator.test.annotations.InjectVendor;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import org.apache.log4j.Logger;

import static com.hazelcast.raft.service.atomiclong.RaftAtomicLongService.SERVICE_NAME;


public class RaftAtomicLongTest {

    /**
     * Returns the name of the configured data-structure. Normally your tests will contain a 'name'
     * property so you can define e.g. 'offheapMap' or 'onheapMap' etc. This way you can change the
     * behavior of the test by switching to a different data-structure.
     */
    @SuppressWarnings("checkstyle:visibilitymodifier")
    public String name = "RaftAtomicLong";

    protected final Logger logger = Logger.getLogger(getClass());

    @InjectVendor
    protected HazelcastInstance targetInstance;

    @InjectTestContext
    protected TestContext testContext;

    // properties
    public int countersLength = 100;

    private IAtomicLong totalCounter;
    private IAtomicLong[] counters;

    @Setup
    public void setup() {
        totalCounter = targetInstance.getAtomicLong(name + ":Total");
        counters = new IAtomicLong[countersLength];

        for (int i = 0; i < countersLength; i++) {
            counters[i] = RaftProxyFactory.create(targetInstance, SERVICE_NAME, name + ":" + i);
        }
    }

    @Prepare
    public void prepare() {
        for (IAtomicLong counter : counters) {
            counter.get();
        }
    }

//    @TimeStep(prob = -1)
//    public void get(ThreadState state) {
//        state.randomCounter().get();
//    }

    @TimeStep(prob = 1)
    public void write(ThreadState state) {
        state.randomCounter().incrementAndGet();
        state.increments++;
    }

    public class ThreadState extends BaseThreadState {

        private long increments;

        private IAtomicLong randomCounter() {
            int index = randomInt(counters.length);
            return counters[index];
        }
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        totalCounter.addAndGet(state.increments);
    }

    @Verify
    public void verify() {
        long actual = 0;
        for (IAtomicLong counter : counters) {
            actual += counter.get();
        }

        long expected = totalCounter.get();
        if (expected != actual) {
            throw new AssertionError("Expected: " + expected + ", Actual: " + actual);
        }
    }

    @Teardown
    public void teardown() {
        for (IAtomicLong counter : counters) {
            counter.destroy();
        }
        totalCounter.destroy();
    }
}
