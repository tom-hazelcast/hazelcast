package com.hazelcast.raft.service.semaphore;

import com.hazelcast.config.Config;
import com.hazelcast.config.raft.RaftConfig;
import com.hazelcast.config.raft.RaftGroupConfig;
import com.hazelcast.config.raft.RaftSemaphoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.service.HazelcastRaftTestSupport;
import com.hazelcast.raft.impl.service.operation.snapshot.RestoreSnapshotOp;
import com.hazelcast.raft.impl.session.RaftSessionService;
import com.hazelcast.raft.service.blocking.ResourceRegistry;
import com.hazelcast.raft.service.semaphore.operation.ReleasePermitsOp;
import com.hazelcast.raft.service.semaphore.proxy.RaftSessionAwareSemaphoreProxy;
import com.hazelcast.raft.service.session.SessionManagerService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RandomPicker;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.raft.impl.RaftUtil.getSnapshotEntry;
import static com.hazelcast.raft.service.session.AbstractSessionManager.NO_SESSION_ID;
import static com.hazelcast.raft.service.spi.RaftProxyFactory.create;
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
public class RaftSemaphoreAdvancedTest extends HazelcastRaftTestSupport {

    private static final int LOG_ENTRY_COUNT_TO_SNAPSHOT = 10;

    private HazelcastInstance[] instances;
    private HazelcastInstance semaphoreInstance;
    private RaftSessionAwareSemaphoreProxy semaphore;
    private String name = "semaphore";
    private int groupSize = 3;

    @Before
    public void setup() {
        instances = createInstances();

        semaphore = createSemaphore(name);
        assertNotNull(semaphore);
    }

    @Test
    public void testSuccessfulTryAcquireClearsWaitTimeouts() {
        semaphore.init(1);

        RaftGroupId groupId = semaphore.getGroupId();
        HazelcastInstance leader = getLeaderInstance(instances, groupId);
        RaftSemaphoreService service = getNodeEngineImpl(leader).getService(RaftSemaphoreService.SERVICE_NAME);
        final SemaphoreRegistry registry = service.getRegistryOrNull(groupId);

        final CountDownLatch latch = new CountDownLatch(1);
        spawn(new Runnable() {
            @Override
            public void run() {
                semaphore.tryAcquire(2, 10, MINUTES);
                latch.countDown();
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertFalse(registry.getWaitTimeouts().isEmpty());
            }
        });

        semaphore.increasePermits(1);

        assertOpenEventually(latch);

        assertTrue(service.getRegistryOrNull(groupId).getWaitTimeouts().isEmpty());
    }

    @Test
    public void testFailedTryAcquireClearsWaitTimeouts() {
        semaphore.init(1);

        RaftGroupId groupId = semaphore.getGroupId();
        HazelcastInstance leader = getLeaderInstance(instances, groupId);
        RaftSemaphoreService service = getNodeEngineImpl(leader).getService(RaftSemaphoreService.SERVICE_NAME);
        SemaphoreRegistry registry = service.getRegistryOrNull(groupId);

        boolean success = semaphore.tryAcquire(2, 1, TimeUnit.SECONDS);

        assertFalse(success);
        assertTrue(registry.getWaitTimeouts().isEmpty());
    }

    @Test
    public void testPermitIncreaseClearsWaitTimeouts() {
        semaphore.init(1);

        RaftGroupId groupId = semaphore.getGroupId();
        HazelcastInstance leader = getLeaderInstance(instances, groupId);
        RaftSemaphoreService service = getNodeEngineImpl(leader).getService(RaftSemaphoreService.SERVICE_NAME);
        final SemaphoreRegistry registry = service.getRegistryOrNull(groupId);

        final CountDownLatch latch = new CountDownLatch(1);
        spawn(new Runnable() {
            @Override
            public void run() {
                semaphore.tryAcquire(2, 10, MINUTES);
                latch.countDown();
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertFalse(registry.getWaitTimeouts().isEmpty());
            }
        });

        semaphore.increasePermits(1);

        assertOpenEventually(latch);
        assertTrue(registry.getWaitTimeouts().isEmpty());
    }

    @Test
    public void testDestroyClearsWaitTimeouts() {
        semaphore.init(1);

        RaftGroupId groupId = semaphore.getGroupId();
        HazelcastInstance leader = getLeaderInstance(instances, groupId);
        RaftSemaphoreService service = getNodeEngineImpl(leader).getService(RaftSemaphoreService.SERVICE_NAME);
        final SemaphoreRegistry registry = service.getRegistryOrNull(groupId);

        spawn(new Runnable() {
            @Override
            public void run() {
                semaphore.tryAcquire(2, 10, MINUTES);
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertFalse(registry.getWaitTimeouts().isEmpty());
            }
        });

        semaphore.destroy();

        assertTrue(registry.getWaitTimeouts().isEmpty());
    }

    @Test
    public void testNewRaftGroupMemberSchedulesTimeoutsWithSnapshot() throws ExecutionException, InterruptedException {
        semaphore.init(1);

        spawn(new Runnable() {
            @Override
            public void run() {
                semaphore.tryAcquire(2, 10, MINUTES);
            }
        });

        for (int i = 0; i < LOG_ENTRY_COUNT_TO_SNAPSHOT; i++) {
            semaphore.acquire();
            semaphore.release();
        }

        final RaftGroupId groupId = semaphore.getGroupId();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                HazelcastInstance leader = getLeaderInstance(instances, groupId);
                RaftSemaphoreService service = getNodeEngineImpl(leader).getService(RaftSemaphoreService.SERVICE_NAME);
                SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
                assertFalse(registry.getWaitTimeouts().isEmpty());
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftNodeImpl raftNode = getRaftNode(instance, groupId);
                    assertNotNull(raftNode);
                    LogEntry snapshotEntry = getSnapshotEntry(raftNode);
                    assertTrue(snapshotEntry.index() > 0);
                    List<RestoreSnapshotOp> ops = (List<RestoreSnapshotOp>) snapshotEntry.operation();
                    for (RestoreSnapshotOp op : ops) {
                        if (op.getServiceName().equals(RaftSemaphoreService.SERVICE_NAME)) {
                            ResourceRegistry registry = (ResourceRegistry) op.getSnapshot();
                            assertFalse(registry.getWaitTimeouts().isEmpty());
                            return;
                        }
                    }
                    fail();
                }
            }
        });

        instances[1].shutdown();

        final HazelcastInstance newInstance = factory.newHazelcastInstance(createConfig(groupSize, groupSize));
        getRaftService(newInstance).triggerRaftMemberPromotion().get();
        getRaftService(newInstance).triggerRebalanceRaftGroups().get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftSemaphoreService service = getNodeEngineImpl(newInstance).getService(RaftSemaphoreService.SERVICE_NAME);
                SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertFalse(registry.getWaitTimeouts().isEmpty());
                assertEquals(1, registry.availablePermits(name));
            }
        });
    }

    @Test
    public void testInactiveSessionsAreEventuallyClosed() throws ExecutionException, InterruptedException {
        semaphore.init(1);
        semaphore.acquire();

        final RaftGroupId groupId = semaphore.getGroupId();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftSessionService sessionService = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                    assertFalse(sessionService.getSessions(groupId).isEmpty());
                }
            }
        });

        NodeEngineImpl nodeEngine = getNodeEngineImpl(semaphoreInstance);
        final SessionManagerService service = nodeEngine.getService(SessionManagerService.SERVICE_NAME);
        long sessionId = service.getSession(groupId);

        assertNotEquals(NO_SESSION_ID, sessionId);

        RaftOp op = new ReleasePermitsOp(name, sessionId, newUnsecureUUID(), 1);
        getRaftInvocationManager(semaphoreInstance).invoke(groupId, op).get();


        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftSessionService service = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                    assertTrue(service.getSessions(groupId).isEmpty());
                }

                assertEquals(NO_SESSION_ID, service.getSession(groupId));
            }
        });
    }

    @Test
    public void testActiveSessionIsNotClosed() {
        semaphore.init(1);
        semaphore.acquire();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftSessionService sessionService = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                    assertFalse(sessionService.getSessions(semaphore.getGroupId()).isEmpty());
                }
            }
        });

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftSessionService sessionService = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                    assertFalse(sessionService.getSessions(semaphore.getGroupId()).isEmpty());
                }
            }
        }, 20);
    }

    @Test
    public void testActiveSessionWithPendingPermitIsNotClosed() {
        spawn(new Runnable() {
            @Override
            public void run() {
                semaphore.acquire();
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftSessionService sessionService = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                    assertFalse(sessionService.getSessions(semaphore.getGroupId()).isEmpty());
                }
            }
        });

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftSessionService sessionService = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                    assertFalse(sessionService.getSessions(semaphore.getGroupId()).isEmpty());
                }
            }
        }, 20);
    }

    protected HazelcastInstance[] createInstances() {
        return newInstances(groupSize);
    }

    private RaftSessionAwareSemaphoreProxy createSemaphore(String name) {
        semaphoreInstance = instances[RandomPicker.getInt(instances.length)];
        return create(semaphoreInstance, RaftSemaphoreService.SERVICE_NAME, name);
    }

    @Override
    protected Config createConfig(int groupSize, int metadataGroupSize) {
        Config config = super.createConfig(groupSize, metadataGroupSize);
        RaftConfig raftConfig = config.getRaftConfig();
        raftConfig.getRaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(LOG_ENTRY_COUNT_TO_SNAPSHOT);
        raftConfig.addGroupConfig(new RaftGroupConfig(name, groupSize));
        raftConfig.setSessionTimeToLiveSeconds(10);
        raftConfig.setSessionHeartbeatIntervalMillis(SECONDS.toMillis(1));

        RaftSemaphoreConfig semaphoreConfig = new RaftSemaphoreConfig(name, name, true);
        config.addRaftSemaphoreConfig(semaphoreConfig);
        return config;
    }
}
