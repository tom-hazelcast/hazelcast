package com.hazelcast.raft.service.lock;

import com.hazelcast.config.Config;
import com.hazelcast.config.raft.RaftGroupConfig;
import com.hazelcast.config.raft.RaftLockConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.exception.RaftGroupDestroyedException;
import com.hazelcast.raft.impl.service.HazelcastRaftTestSupport;
import com.hazelcast.raft.service.lock.proxy.RaftLockProxy;
import com.hazelcast.raft.service.session.SessionManagerService;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.RandomPicker;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.concurrent.lock.LockTestUtils.lockByOtherThread;
import static com.hazelcast.raft.service.lock.RaftLockService.SERVICE_NAME;
import static com.hazelcast.raft.service.lock.RaftLockService.WAIT_TIMEOUT_TASK_UPPER_BOUND_MILLIS;
import static com.hazelcast.raft.service.spi.RaftProxyFactory.create;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftLockBasicTest extends HazelcastRaftTestSupport {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private HazelcastInstance[] instances;
    private ILock lock;
    private String name = "lock";
    private String groupName = "lock";
    private int groupSize = 3;

    @Before
    public void setup() {
        instances = createInstances();

        lock = createLock(name);
        assertNotNull(lock);
    }

    protected HazelcastInstance[] createInstances() {
        return newInstances(groupSize);
    }

    protected ILock createLock(String name) {
        return create(instances[RandomPicker.getInt(instances.length)], RaftLockService.SERVICE_NAME, name);
    }

    @Test
    public void testLock_whenNotLocked() {
        lock.lock();
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLockCount());
    }

    @Test
    public void testLock_whenLockedBySelf() {
        lock.lock();
        lock.lock();
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(2, lock.getLockCount());
    }

    @Test
    public void testLock_whenLockedByOther() throws InterruptedException {
        lock.lock();
        assertTrue(lock.isLocked());
        assertEquals(1, lock.getLockCount());
        assertTrue(lock.isLockedByCurrentThread());

        final CountDownLatch latch = new CountDownLatch(1);

        Thread t = new Thread() {
            public void run() {
                lock.lock();
                latch.countDown();
            }
        };

        t.start();
        assertFalse(latch.await(3000, TimeUnit.MILLISECONDS));
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void testUnlock_whenFree() {
        lock.unlock();
    }

    @Test
    public void testUnlock_whenLockedBySelf() {
        lock.lock();

        lock.unlock();

        assertFalse(lock.isLocked());
        assertEquals(0, lock.getLockCount());
    }

    @Test
    public void testUnlock_whenReentrantlyLockedBySelf() {
        lock.lock();
        lock.lock();

        lock.unlock();

        assertTrue(lock.isLockedByCurrentThread());
        assertTrue(lock.isLocked());
        assertEquals(1, lock.getLockCount());
    }

    @Test
    public void testLock_Unlock_thenLock() throws Exception {
        lock.lock();
        lock.unlock();

        spawn(new Runnable() {
            @Override
            public void run() {
                lock.lock();
            }
        }).get(1, TimeUnit.MINUTES);

        assertEquals(1, lock.getLockCount());
    }

    @Test
    public void testUnlock_whenPendingLockOfOtherThread() throws InterruptedException {
        lock.lock();
        final CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                lock.lock();
                latch.countDown();

            }
        });
        thread.start();

        sleepSeconds(1);
        lock.unlock();
        latch.await();

        assertTrue(lock.isLocked());
        assertFalse(lock.isLockedByCurrentThread());
    }

    @Test
    public void testUnlock_whenLockedByOther() {
        lockByOtherThread(lock);

        try {
            lock.unlock();
            fail();
        } catch (IllegalMonitorStateException expected) {
        }

        assertTrue(lock.isLocked());
        assertEquals(1, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testTryLock_whenNotLocked() {
        boolean result = lock.tryLock();

        assertTrue(result);
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testTryLock_whenLockedBySelf() {
        lock.lock();

        boolean result = lock.tryLock();

        assertTrue(result);
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(2, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testTryLock_whenLockedByOther() {
        lockByOtherThread(lock);

        boolean result = lock.tryLock();

        assertFalse(result);
        assertFalse(lock.isLockedByCurrentThread());
        assertTrue(lock.isLocked());
        assertEquals(1, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testTryLockTimeout() throws InterruptedException {
        boolean result = lock.tryLock(1, TimeUnit.SECONDS);

        assertTrue(result);
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testTryLockTimeout_whenLockedBySelf() throws InterruptedException {
        lock.lock();

        boolean result = lock.tryLock(1, TimeUnit.SECONDS);

        assertTrue(result);
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(2, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testTryLockTimeout_whenLockedByOther() throws InterruptedException {
        lockByOtherThread(lock);

        boolean result = lock.tryLock(100, TimeUnit.MILLISECONDS);

        assertFalse(result);
        assertFalse(lock.isLockedByCurrentThread());
        assertTrue(lock.isLocked());
        assertEquals(1, lock.getLockCount());
    }

    @Test(timeout = 60000)
    public void testTryLockLongTimeout_whenLockedByOther() throws InterruptedException {
        lockByOtherThread(lock);

        boolean result = lock.tryLock(WAIT_TIMEOUT_TASK_UPPER_BOUND_MILLIS + 1, TimeUnit.MILLISECONDS);

        assertFalse(result);
        assertFalse(lock.isLockedByCurrentThread());
        assertTrue(lock.isLocked());
        assertEquals(1, lock.getLockCount());
    }

    @Test
    public void testCreate_withDefaultGroup() {
        ILock lock = createLock(randomName());
        assertEquals(RaftGroupConfig.DEFAULT_GROUP, getGroupId(lock).name());
    }

    @Test(expected = DistributedObjectDestroyedException.class)
    public void testUse_afterDestroy() {
        lock.destroy();
        lock.lock();
    }

    @Test
    public void testLockAutoRelease_whenShutdownGracefully() {
        ILock lock0 = create(instances[0], SERVICE_NAME, name);
        ILock lock1 = create(instances[1], SERVICE_NAME, name);

        lock0.lock();

        instances[0].shutdown();

        assertFalse(lock1.isLocked());
        assertTrue(lock1.tryLock());
    }

    @Test
    public void testLock_whenSessionManagerShutdown() {
        ILock lock = create(instances[0], SERVICE_NAME, name);
        SessionManagerService sessionManager = getNodeEngineImpl(instances[0]).getService(SessionManagerService.SERVICE_NAME);

        boolean success = sessionManager.onShutdown(60, TimeUnit.SECONDS);
        assertTrue(success);

        exception.expect(IllegalStateException.class);
        lock.lock();
    }

    @Test(expected = DistributedObjectDestroyedException.class)
    public void testCreate_afterDestroy() {
        lock.destroy();

        lock = createLock(name);
        lock.lock();
    }

    @Test
    public void testMultipleDestroy() {
        lock.destroy();
        lock.destroy();
    }

    @Test
    public void testWaitEntries_afterDestroy() throws Exception {
        lock.lock();

        Collection<Future> futures = new ArrayList<Future>();
        for (int i = 0; i < 4; i++) {
            Future<Object> future = spawn(new Callable<Object>() {
                @Override
                public Object call() {
                    lock.lock();
                    return null;
                }
            });
            futures.add(future);
        }

        sleepSeconds(1);
        lock.destroy();

        for (Future future : futures) {
            try {
                future.get();
                fail("Lock acquire should fail!");
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof DistributedObjectDestroyedException) {
                    // expected when destroyed while waiting
                    continue;
                }
                if (cause instanceof IllegalStateException) {
                    // expected when lock called after destroyed
                    continue;
                }
                throw ExceptionUtil.rethrow(e);
            }
        }
    }

    @Test
    public void testRecreate_afterGroupDestroy() throws Exception {
        lock.destroy();

        final RaftGroupId groupId = getGroupId(lock);
        getRaftInvocationManager(instances[0]).triggerDestroyRaftGroup(groupId).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                lock = createLock(name);
                RaftGroupId newGroupId = getGroupId(lock);
                assertNotEquals(groupId, newGroupId);
            }
        });

        lock.lock();
    }

    @Test
    public void testWaitEntries_afterGroupDestroy() throws Exception {
        lock.lock();

        Collection<Future> futures = new ArrayList<Future>();
        for (int i = 0; i < 4; i++) {
            Future<Object> future = spawn(new Callable<Object>() {
                @Override
                public Object call() {
                    lock.lock();
                    return null;
                }
            });
            futures.add(future);
        }

        sleepSeconds(1);

        final RaftGroupId groupId = getGroupId(lock);
        getRaftInvocationManager(instances[0]).triggerDestroyRaftGroup(groupId).get();

        for (Future future : futures) {
            try {
                future.get();
                fail("Lock acquire should fail!");
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof DistributedObjectDestroyedException) {
                    // expected when destroyed while waiting
                    continue;
                }
                if (cause instanceof RaftGroupDestroyedException) {
                    // expected when lock called after destroyed
                    continue;
                }
                throw ExceptionUtil.rethrow(e);
            }
        }
    }

    protected RaftGroupId getGroupId(ILock lock) {
        return ((RaftLockProxy) lock).getGroupId();
    }

    @Override
    protected Config createConfig(int groupSize, int metadataGroupSize) {
        Config config = super.createConfig(groupSize, metadataGroupSize);
        config.getRaftConfig().addGroupConfig(new RaftGroupConfig(groupName, groupSize));

        RaftLockConfig lockConfig = new RaftLockConfig(name, groupName);
        config.addRaftLockConfig(lockConfig);
        return config;
    }
}
