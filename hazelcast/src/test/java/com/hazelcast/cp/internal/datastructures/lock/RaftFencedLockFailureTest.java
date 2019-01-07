/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.cp.internal.datastructures.lock;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.datastructures.exception.WaitKeyCancelledException;
import com.hazelcast.cp.internal.datastructures.lock.operation.LockOp;
import com.hazelcast.cp.internal.datastructures.lock.operation.TryLockOp;
import com.hazelcast.cp.internal.datastructures.lock.operation.UnlockOp;
import com.hazelcast.cp.internal.datastructures.lock.proxy.RaftFencedLockProxy;
import com.hazelcast.cp.internal.datastructures.spi.blocking.WaitKeyContainer;
import com.hazelcast.cp.internal.datastructures.spi.blocking.operation.ExpireWaitKeysOp;
import com.hazelcast.cp.internal.session.AbstractProxySessionManager;
import com.hazelcast.cp.internal.session.ProxySessionManagerService;
import com.hazelcast.cp.internal.util.Tuple2;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RandomPicker;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.cp.FencedLock.INVALID_FENCE;
import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;
import static com.hazelcast.util.ThreadUtil.getThreadId;
import static com.hazelcast.util.UuidUtil.newUnsecureUUID;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftFencedLockFailureTest extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;
    private HazelcastInstance lockInstance;
    private RaftFencedLockProxy lock;
    private String objectName = "lock";
    private String proxyName = objectName + "@group1";

    @Before
    public void setup() {
        instances = newInstances(3);
        lockInstance = instances[RandomPicker.getInt(instances.length)];
        lock = (RaftFencedLockProxy) lockInstance.getCPSubsystem().getLock(proxyName);
    }

    private AbstractProxySessionManager getSessionManager() {
        return getNodeEngineImpl(lockInstance).getService(ProxySessionManagerService.SERVICE_NAME);
    }

    @Test
    public void testRetriedLockDoesNotCancelPendingLockRequest() {
        lockByOtherThread();

        // there is a session id now

        final CPGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        UUID invUid = newUnsecureUUID();

        invocationManager.invoke(groupId, new TryLockOp(objectName, sessionId, getThreadId(), invUid, MINUTES.toMillis(5)));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftLockService service = getNodeEngineImpl(lockInstance).getService(RaftLockService.SERVICE_NAME);
                RaftLockRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertEquals(1, registry.getWaitTimeouts().size());
            }
        });

        invocationManager.invoke(groupId, new LockOp(objectName, sessionId, getThreadId(), invUid));

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                RaftLockService service = getNodeEngineImpl(lockInstance).getService(RaftLockService.SERVICE_NAME);
                RaftLockRegistry registry = service.getRegistryOrNull(groupId);
                assertEquals(1, registry.getWaitTimeouts().size());
            }
        }, 10);
    }

    @Test(timeout = 30000)
    public void testNewLockCancelsPendingLockRequest() {
        lockByOtherThread();

        // there is a session id now

        final CPGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        UUID invUid1 = newUnsecureUUID();
        UUID invUid2 = newUnsecureUUID();

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new TryLockOp(objectName, sessionId, getThreadId(), invUid1, MINUTES.toMillis(5)));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftLockService service = getNodeEngineImpl(lockInstance).getService(RaftLockService.SERVICE_NAME);
                RaftLockRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertEquals(1, registry.getWaitTimeouts().size());
            }
        });

        invocationManager.invoke(groupId, new LockOp(objectName, sessionId, getThreadId(), invUid2));

        try {
            f.join();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test
    public void testRetriedTryLockWithTimeoutDoesNotCancelPendingLockRequest() {
        lockByOtherThread();

        // there is a session id now

        final CPGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        UUID invUid = newUnsecureUUID();

        invocationManager.invoke(groupId, new TryLockOp(objectName, sessionId, getThreadId(), invUid, MINUTES.toMillis(5)));

        final NodeEngineImpl nodeEngine = getNodeEngineImpl(lockInstance);
        final RaftLockService service = nodeEngine.getService(RaftLockService.SERVICE_NAME);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftLockRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertNotNull(registry.getResourceOrNull(objectName));
                assertEquals(1, registry.getWaitTimeouts().size());
            }
        });

        invocationManager.invoke(groupId, new TryLockOp(objectName, sessionId, getThreadId(), invUid, MINUTES.toMillis(5)));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                final int partitionId = nodeEngine.getPartitionService().getPartitionId(groupId);
                final RaftLockRegistry registry = service.getRegistryOrNull(groupId);
                final boolean[] verified = new boolean[1];
                final CountDownLatch latch = new CountDownLatch(1);
                OperationServiceImpl operationService = (OperationServiceImpl) nodeEngine.getOperationService();
                operationService.execute(new PartitionSpecificRunnable() {
                    @Override
                    public int getPartitionId() {
                        return partitionId;
                    }

                    @Override
                    public void run() {
                        RaftLock raftLock = registry.getResourceOrNull(objectName);
                        final Map<Object, WaitKeyContainer<LockInvocationKey>> waitKeys = raftLock.getWaitKeys();
                        verified[0] = (waitKeys.size() == 1 && waitKeys.values().iterator().next().retryCount() == 1);
                        latch.countDown();
                    }
                });

                latch.await(60, SECONDS);

                assertTrue(verified[0]);
            }
        });
    }

    @Test(timeout = 30000)
    public void testNewTryLockWithTimeoutCancelsPendingLockRequest() {
        lockByOtherThread();

        // there is a session id now

        final CPGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        UUID invUid1 = newUnsecureUUID();
        UUID invUid2 = newUnsecureUUID();

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new TryLockOp(objectName, sessionId, getThreadId(), invUid1, MINUTES.toMillis(5)));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftLockService service = getNodeEngineImpl(lockInstance).getService(RaftLockService.SERVICE_NAME);
                RaftLockRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertEquals(1, registry.getWaitTimeouts().size());
            }
        });

        invocationManager.invoke(groupId, new TryLockOp(objectName, sessionId, getThreadId(), invUid2, MINUTES.toMillis(5)));

        try {
            f.join();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test
    public void testRetriedTryLockWithoutTimeoutDoesNotCancelPendingLockRequest() {
        lockByOtherThread();

        // there is a session id now

        final CPGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        UUID invUid = newUnsecureUUID();

        invocationManager.invoke(groupId, new TryLockOp(objectName, sessionId, getThreadId(), invUid, MINUTES.toMillis(5)));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftLockService service = getNodeEngineImpl(lockInstance).getService(RaftLockService.SERVICE_NAME);
                RaftLockRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertEquals(1, registry.getWaitTimeouts().size());
            }
        });

        invocationManager.invoke(groupId, new TryLockOp(objectName, sessionId, getThreadId(), invUid, 0));

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                RaftLockService service = getNodeEngineImpl(lockInstance).getService(RaftLockService.SERVICE_NAME);
                RaftLockRegistry registry = service.getRegistryOrNull(groupId);
                assertEquals(1, registry.getWaitTimeouts().size());
            }
        }, 10);
    }

    @Test(timeout = 30000)
    public void testNewUnlockCancelsPendingLockRequest() {
        lockByOtherThread();

        // there is a session id now

        final CPGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        UUID invUid = newUnsecureUUID();

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new TryLockOp(objectName, sessionId, getThreadId(), invUid, MINUTES.toMillis(5)));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftLockService service = getNodeEngineImpl(lockInstance).getService(RaftLockService.SERVICE_NAME);
                RaftLockRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertEquals(1, registry.getWaitTimeouts().size());
            }
        });

        try {
            lock.unlock();
            fail();
        } catch (IllegalMonitorStateException ignored) {
        }

        try {
            f.join();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test
    public void testLockAcquireRetry() {
        lock.lock();
        lock.unlock();

        // there is a session id now

        CPGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        UUID invUid = newUnsecureUUID();

        invocationManager.invoke(groupId, new LockOp(objectName, sessionId, getThreadId(), invUid)).join();
        invocationManager.invoke(groupId, new LockOp(objectName, sessionId, getThreadId(), invUid)).join();

        assertEquals(1, lock.getLockCountIfLockedByCurrentThread());
    }

    @Test
    public void testLockReentrantAcquireRetry() {
        lock.lock();
        lock.unlock();

        // there is a session id now

        CPGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        UUID invUid1 = newUnsecureUUID();
        UUID invUid2 = newUnsecureUUID();

        invocationManager.invoke(groupId, new LockOp(objectName, sessionId, getThreadId(), invUid1)).join();
        invocationManager.invoke(groupId, new LockOp(objectName, sessionId, getThreadId(), invUid2)).join();
        invocationManager.invoke(groupId, new LockOp(objectName, sessionId, getThreadId(), invUid2)).join();

        assertEquals(2, lock.getLockCountIfLockedByCurrentThread());
    }

    @Test
    public void testRetriedUnlockIsSuccessfulAfterLockedByAnotherEndpoint() {
        lock.lock();

        CPGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        UUID invUid = newUnsecureUUID();

        invocationManager.invoke(groupId, new UnlockOp(objectName, sessionId, getThreadId(), invUid, 1)).join();

        lockByOtherThread();

        invocationManager.invoke(groupId, new UnlockOp(objectName, sessionId, getThreadId(), invUid, 1)).join();
    }

    @Test
    public void testIsLockedByCurrentThreadCallInitializesLocalLockState() {
        lock.lock();
        lock.unlock();

        // there is a session id now

        final CPGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        invocationManager.invoke(groupId, new LockOp(objectName, sessionId, getThreadId(), newUnsecureUUID())).join();

        // the current thread acquired the lock once and we pretend that there was a operation timeout in lock.lock() call

        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLocalLockCount());
        assertNotEquals(INVALID_FENCE, lock.getLocalLockFence());
    }

    @Test
    public void testIsLockedByCurrentThreadCallInitializesLocalReentrantLockState() {
        lock.lock();
        lock.unlock();

        // there is a session id now

        final CPGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        invocationManager.invoke(groupId, new LockOp(objectName, sessionId, getThreadId(), newUnsecureUUID())).join();
        invocationManager.invoke(groupId, new LockOp(objectName, sessionId, getThreadId(), newUnsecureUUID())).join();

        // the current thread acquired the lock twice and we pretend that both lock.lock() calls failed with operation timeout

        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(2, lock.getLocalLockCount());
        assertNotEquals(INVALID_FENCE, lock.getLocalLockFence());
    }

    @Test
    public void testLockCallInitializesLocalReentrantLockState() {
        lock.lock();
        lock.unlock();

        // there is a session id now

        final CPGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        invocationManager.invoke(groupId, new LockOp(objectName, sessionId, getThreadId(), newUnsecureUUID())).join();
        invocationManager.invoke(groupId, new LockOp(objectName, sessionId, getThreadId(), newUnsecureUUID())).join();

        lock.lock();

        assertEquals(3, lock.getLocalLockCount());
        assertNotEquals(INVALID_FENCE, lock.getLocalLockFence());
    }

    @Test
    public void testUnlockReleasesObservedAcquiresOneByOne() {
        lock.lock();
        lock.unlock();

        // there is a session id now

        final CPGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        invocationManager.invoke(groupId, new LockOp(objectName, sessionId, getThreadId(), newUnsecureUUID())).join();

        lock.lock();

        lock.unlock();

        assertEquals(1, lock.getLocalLockCount());
        assertNotEquals(INVALID_FENCE, lock.getLocalLockFence());

        lock.unlock();

        assertEquals(0, lock.getLocalLockCount());
        assertEquals(INVALID_FENCE, lock.getLocalLockFence());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftLockService service = getNodeEngineImpl(lockInstance).getService(RaftLockService.SERVICE_NAME);
                RaftLockRegistry registry = service.getRegistryOrNull(groupId);
                assertFalse(registry.getLockOwnershipState(objectName).isLocked());
            }
        });
    }

    @Test
    public void testUnlockReleasesNotObservedAcquiresAllAtOnce() {
        lock.lock();
        lock.unlock();

        // there is a session id now

        final CPGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        invocationManager.invoke(groupId, new LockOp(objectName, sessionId, getThreadId(), newUnsecureUUID())).join();
        invocationManager.invoke(groupId, new LockOp(objectName, sessionId, getThreadId(), newUnsecureUUID())).join();

        lock.unlock();

        assertFalse(lock.isLockedByCurrentThread());
        assertEquals(0, lock.getLocalLockCount());
        assertEquals(INVALID_FENCE, lock.getLocalLockFence());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftLockService service = getNodeEngineImpl(lockInstance).getService(RaftLockService.SERVICE_NAME);
                RaftLockRegistry registry = service.getRegistryOrNull(groupId);
                assertFalse(registry.getLockOwnershipState(objectName).isLocked());
            }
        });
    }

    @Test
    public void testRetriedWaitKeysAreExpiredTogether() {
        final CountDownLatch releaseLatch = new CountDownLatch(1);
        spawn(new Runnable() {
            @Override
            public void run() {
                lock.lock();
                assertOpenEventually(releaseLatch);
                lock.unlock();
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(lock.isLocked());
            }
        });

        // there is a session id now

        final CPGroupId groupId = lock.getGroupId();
        long sessionId = getSessionManager().getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        RaftInvocationManager invocationManager = getRaftInvocationManager(lockInstance);
        UUID invUid = newUnsecureUUID();
        final Tuple2[] lockWaitTimeoutKeyRef = new Tuple2[1];

        InternalCompletableFuture<RaftLockOwnershipState> f1 = invocationManager
                .invoke(groupId, new TryLockOp(objectName, sessionId, getThreadId(), invUid, SECONDS.toMillis(300)));

        final NodeEngineImpl nodeEngine = getNodeEngineImpl(lockInstance);
        final RaftLockService service = nodeEngine.getService(RaftLockService.SERVICE_NAME);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftLockRegistry registry = service.getRegistryOrNull(groupId);
                Map<Tuple2<String, UUID>, Tuple2<Long, Long>> waitTimeouts = registry.getWaitTimeouts();
                assertEquals(1, waitTimeouts.size());
                lockWaitTimeoutKeyRef[0] = waitTimeouts.keySet().iterator().next();
            }
        });

        InternalCompletableFuture<RaftLockOwnershipState> f2 = invocationManager
                .invoke(groupId, new TryLockOp(objectName, sessionId, getThreadId(), invUid, SECONDS.toMillis(300)));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                final int partitionId = nodeEngine.getPartitionService().getPartitionId(groupId);
                final RaftLockRegistry registry = service.getRegistryOrNull(groupId);
                final boolean[] verified = new boolean[1];
                final CountDownLatch latch = new CountDownLatch(1);
                OperationServiceImpl operationService = (OperationServiceImpl) nodeEngine.getOperationService();
                operationService.execute(new PartitionSpecificRunnable() {
                    @Override
                    public int getPartitionId() {
                        return partitionId;
                    }

                    @Override
                    public void run() {
                        RaftLock raftLock = registry.getResourceOrNull(objectName);
                        final Map<Object, WaitKeyContainer<LockInvocationKey>> waitKeys = raftLock.getWaitKeys();
                        verified[0] = (waitKeys.size() == 1 && waitKeys.values().iterator().next().retryCount() == 1);
                        latch.countDown();
                    }
                });

                latch.await(60, SECONDS);

                assertTrue(verified[0]);
            }
        });

        RaftOp op = new ExpireWaitKeysOp(RaftLockService.SERVICE_NAME,
                Collections.<Tuple2<String, UUID>>singletonList(lockWaitTimeoutKeyRef[0]));
        invocationManager.invoke(groupId, op).join();

        assertTrue(service.getRegistryOrNull(groupId).getWaitTimeouts().isEmpty());

        releaseLatch.countDown();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertFalse(lock.isLocked());
            }
        });

        RaftLockOwnershipState response1 = f1.join();
        RaftLockOwnershipState response2 = f2.join();

        assertFalse(response1.isLocked());
        assertFalse(response2.isLocked());
    }

    public void lockByOtherThread() {
        Thread t = new Thread() {
            public void run() {
                try {
                    lock.lock();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        t.start();
        try {
            t.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
