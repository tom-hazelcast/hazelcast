package com.hazelcast.raft.service.lock;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.HazelcastRaftTestSupport;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.service.lock.exception.LockRequestCancelledException;
import com.hazelcast.raft.service.lock.operation.LockOp;
import com.hazelcast.raft.service.lock.operation.TryLockOp;
import com.hazelcast.raft.service.lock.proxy.RaftLockProxy;
import com.hazelcast.raft.service.session.AbstractSessionManager;
import com.hazelcast.raft.service.session.SessionManagerService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RandomPicker;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static com.hazelcast.concurrent.lock.LockTestUtils.lockByOtherThread;
import static com.hazelcast.raft.service.spi.RaftProxyFactory.create;
import static com.hazelcast.util.ThreadUtil.getThreadId;
import static com.hazelcast.util.UuidUtil.newUnsecureUUID;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftLockAdvancedTest extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;
    private HazelcastInstance lockInstance;
    private RaftLockProxy lock;
    private String name = "lock";
    private int groupSize = 3;

    @Before
    public void setup() {
        instances = newInstances(groupSize);
        lock = createLock(name);
        assertNotNull(lock);
    }

    private RaftLockProxy createLock(String name) {
        lockInstance = instances[RandomPicker.getInt(instances.length)];
        return create(lockInstance, RaftLockService.SERVICE_NAME, name);
    }

    private AbstractSessionManager getSessionManager() {
        return getNodeEngineImpl(lockInstance).getService(SessionManagerService.SERVICE_NAME);
    }

    @Test
    public void testLock() {
        lockByOtherThread(lock);

        // there is a session id now

        RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        UUID invUid1 = newUnsecureUUID();
        UUID invUid2 = newUnsecureUUID();

        InternalCompletableFuture<Object> f1 = invocationManager.invoke(groupId, new LockOp(name, sessionId, getThreadId(), invUid1));
        InternalCompletableFuture<Object> f2 = invocationManager.invoke(groupId, new LockOp(name, sessionId, getThreadId(), invUid1));

        invocationManager.invoke(groupId, new LockOp(name, sessionId, getThreadId(), invUid2));

        try {
            f1.join();
            fail();
        } catch (LockRequestCancelledException ignored) {
        }

        try {
            f2.join();
            fail();
        } catch (LockRequestCancelledException ignored) {
        }
    }

    @Test
    public void testTryLock() {
        lockByOtherThread(lock);

        // there is a session id now

        RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        UUID invUid1 = newUnsecureUUID();
        UUID invUid2 = newUnsecureUUID();

        InternalCompletableFuture<Object> f1 =
                invocationManager.invoke(groupId, new TryLockOp(name, sessionId, getThreadId(), invUid1, MINUTES.toMillis(5)));
        InternalCompletableFuture<Object> f2 =
                invocationManager.invoke(groupId, new TryLockOp(name, sessionId, getThreadId(), invUid1, MINUTES.toMillis(5)));

        invocationManager.invoke(groupId, new LockOp(name, sessionId, getThreadId(), invUid2));

        try {
            f1.join();
            fail();
        } catch (LockRequestCancelledException ignored) {
        }

        try {
            f2.join();
            fail();
        } catch (LockRequestCancelledException ignored) {
        }
    }

    @Test
    public void testUnlock() {
        lockByOtherThread(lock);

        // there is a session id now

        RaftGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        UUID invUid1 = newUnsecureUUID();

        InternalCompletableFuture<Object> f1 = invocationManager.invoke(groupId, new LockOp(name, sessionId, getThreadId(), invUid1));

        try {
            lock.unlock();
            fail();
        } catch (IllegalMonitorStateException ignored) {
        }

        try {
            f1.join();
            fail();
        } catch (LockRequestCancelledException ignored) {
        }
    }

}
