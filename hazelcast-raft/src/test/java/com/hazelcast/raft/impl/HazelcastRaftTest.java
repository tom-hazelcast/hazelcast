package com.hazelcast.raft.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.RaftMember;
import com.hazelcast.raft.impl.service.RaftDataService;
import com.hazelcast.raft.impl.service.RaftAddOperation;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.UuidUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.raft.impl.RaftDataSerializerHook.APPEND_REQUEST_OP;
import static com.hazelcast.raft.impl.RaftDataSerializerHook.APPEND_SUCCESS_RESPONSE_OP;
import static com.hazelcast.raft.impl.RaftDataSerializerHook.VOTE_RESPONSE_OP;
import static com.hazelcast.raft.impl.RaftService.METADATA_RAFT;
import static com.hazelcast.raft.impl.RaftUtil.getCommitIndex;
import static com.hazelcast.raft.impl.RaftUtil.getLastLogEntry;
import static com.hazelcast.raft.impl.RaftUtil.getLeaderEndpoint;
import static com.hazelcast.raft.impl.RaftUtil.getRaftNode;
import static com.hazelcast.raft.impl.RaftUtil.getRole;
import static com.hazelcast.raft.impl.RaftUtil.getTerm;
import static com.hazelcast.raft.impl.RaftUtil.waitUntilLeaderElected;
import static com.hazelcast.spi.properties.GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsBetween;
import static com.hazelcast.test.SplitBrainTestSupport.blockCommunicationBetween;
import static com.hazelcast.test.SplitBrainTestSupport.unblockCommunicationBetween;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class HazelcastRaftTest extends HazelcastTestSupport {

    private HazelcastInstance[] instances;
    private Address[] raftAddresses;
    private TestHazelcastInstanceFactory factory;

    @Before
    public void init() {
        factory = createHazelcastInstanceFactory();
    }

    @Test
    public void testTwoNodes_electLeader() {
        testLeaderElection(2);
    }

    @Test
    public void testThreeNodes_electLeader() {
        testLeaderElection(3);
    }

    private void testLeaderElection(int nodeCount) {
        raftAddresses = createRaftAddresses(nodeCount);
        instances = newInstances(raftAddresses);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                RaftNode leaderNode = getLeaderNode(METADATA_RAFT);
                int leaderTerm = getTerm(leaderNode);

                for (HazelcastInstance instance : instances) {
                    RaftNode raftNode = getRaftNode(instance, METADATA_RAFT);
                    assertEquals(leaderNode.getLocalEndpoint(), getLeaderEndpoint(raftNode));
                    assertEquals(leaderTerm, getTerm(raftNode));
                }
            }
        }, 30);
    }

    @Test
    public void testTwoNodes_commitEntry() throws ExecutionException, InterruptedException {
        raftAddresses = createRaftAddresses(2);
        instances = newInstances(raftAddresses);

        RaftNode leader = getLeaderNode(METADATA_RAFT);
        final Object val = "val";
        Future f = leader.replicate(new RaftAddOperation(val));

        f.get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (HazelcastInstance instance : instances) {
                    assertEquals(1, getCommitIndex(getRaftNode(instance, METADATA_RAFT)));
                    RaftDataService service = getRaftTestService(instance);
                    assertEquals(val, service.get(1));
                }
            }
        });
    }

    @Test
    public void testTwoNodes_doNotCommitUntilBothNodesAppend() throws ExecutionException, InterruptedException {
        raftAddresses = createRaftAddresses(2);
        instances = newInstances(raftAddresses);

        RaftNode leader = getLeaderNode(METADATA_RAFT);

        HazelcastInstance leaderInstance = getLeaderInstance(leader);
        HazelcastInstance[] followers = getAllInstancesExcept(leaderInstance);

        dropOperationsBetween(leaderInstance, followers[0], RaftDataSerializerHook.F_ID, singletonList(APPEND_REQUEST_OP));

        final Object val = "val";
        Future f = leader.replicate(new RaftAddOperation(val));

        try {
            f.get(10, TimeUnit.SECONDS);
            fail();
        } catch (TimeoutException ignored) {
        }

        for (HazelcastInstance instance : instances) {
            assertEquals(0, getCommitIndex(getRaftNode(instance, METADATA_RAFT)));
        }
    }

    @Test
    public void testTwoNodes_commitMultipleEntriesSequentially() throws ExecutionException, InterruptedException {
        raftAddresses = createRaftAddresses(2);
        instances = newInstances(raftAddresses);

        RaftNode leader = getLeaderNode(METADATA_RAFT);

        final int entryCount = 100;
        for (int i = 0; i < entryCount; i++) {
            final Object val = "val" + i;
            Future f = leader.replicate(new RaftAddOperation(val));
            f.get();
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (HazelcastInstance instance : instances) {
                    assertEquals(entryCount, getCommitIndex(getRaftNode(instance, METADATA_RAFT)));
                    RaftDataService service = getRaftTestService(instance);
                    for (int i = 0; i < entryCount; i++) {
                        int commitIndex = i + 1;
                        Object val = "val" + i;
                        assertEquals(val, service.get(commitIndex));
                    }
                }
            }
        });
    }

    @Test
    public void testTwoNodes_commitMultipleEntries() throws ExecutionException, InterruptedException {
        raftAddresses = createRaftAddresses(2);
        instances = newInstances(raftAddresses);

        RaftNode leader = getLeaderNode(METADATA_RAFT);

        final int entryCount = 100;
        List<Future> futures = new ArrayList<Future>(entryCount);
        for (int i = 0; i < entryCount; i++) {
            final Object val = "val" + i;
            futures.add(leader.replicate(new RaftAddOperation(val)));
        }

        for (Future f : futures) {
            f.get();
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (HazelcastInstance instance : instances) {
                    assertEquals(entryCount, getCommitIndex(getRaftNode(instance, METADATA_RAFT)));
                    RaftDataService service = getRaftTestService(instance);
                    Set<Object> values = service.values();
                    for (int i = 0; i < entryCount; i++) {
                        Object val = "val" + i;
                        assertTrue(values.contains(val));
                    }
                }
            }
        });
    }

    @Test
    public void testNextLeader_commitsPreviousLeadersEntry() throws Exception {
        raftAddresses = createRaftAddresses(3);
        instances = newInstances(raftAddresses);

        final RaftNode leader = getLeaderNode(METADATA_RAFT);

        Future f1 = leader.replicate(new RaftAddOperation("val1"));
        f1.get();

        HazelcastInstance leaderInstance = getLeaderInstance(leader);
        final HazelcastInstance[] followerInstances = getAllInstancesExcept(leaderInstance);

        final int commitIndex = getCommitIndex(leader);

        for (HazelcastInstance followerInstance : followerInstances) {
            dropOperationsBetween(followerInstance, leaderInstance, RaftDataSerializerHook.F_ID, asList(
                    APPEND_SUCCESS_RESPONSE_OP, VOTE_RESPONSE_OP));
        }

        Future f2 = leader.replicate(new RaftAddOperation("val2"));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (HazelcastInstance instance : instances) {
                    RaftNode raftNode = getRaftNode(instance, METADATA_RAFT);
                    LogEntry lastLogEntry = getLastLogEntry(raftNode);
                    assertTrue(lastLogEntry.index() > commitIndex);
                }
            }
        });

        // the followers append f2 but the leader does not commit because it does not receive AppendResponse
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (HazelcastInstance instance : instances) {
                    RaftNode raftNode = getRaftNode(instance, METADATA_RAFT);
                    assertEquals(commitIndex, getCommitIndex(raftNode));
                }
            }
        });

        block(leaderInstance, followerInstances);

        assertLeaderNotEqualsEventually(leader, followerInstances);

        unblock(leaderInstance, followerInstances);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                RaftNode raftNode = getRaftNode(followerInstances[0], METADATA_RAFT);
                assertEquals(getLeaderEndpoint(leader), getLeaderEndpoint(raftNode));
            }
        });

        // the new leader appends f3 to the next index of f2, and commits both f2 and f3
        RaftNode newLeader = getLeaderNode(METADATA_RAFT);
        Future f3 = newLeader.replicate(new RaftAddOperation("val3"));

        assertEquals("val3", f3.get());
        try {
            f2.get();
            fail();
        } catch (Exception ignored) {
        }
    }

    @Test
    public void testNextLeader_invalidatesPreviousLeadersEntry() throws Exception {
        raftAddresses = createRaftAddresses(3);
        instances = newInstances(raftAddresses);

        final RaftNode leader = getLeaderNode(METADATA_RAFT);

        Future f1 = leader.replicate(new RaftAddOperation("val1"));
        f1.get();

        HazelcastInstance leaderInstance = getLeaderInstance(leader);
        final HazelcastInstance[] followerInstances = getAllInstancesExcept(leaderInstance);

        final int commitIndex = getCommitIndex(leader);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (HazelcastInstance instance : instances) {
                    RaftNode raftNode = getRaftNode(instance, METADATA_RAFT);
                    assertEquals(commitIndex, getCommitIndex(raftNode));
                }
            }
        });

        block(leaderInstance, followerInstances);

        // the alone leader appends f2 but cannot replicate it to the others
        Future f2 = leader.replicate(new RaftAddOperation("val2"));

        assertLeaderNotEqualsEventually(leader, followerInstances);

        unblock(leaderInstance, followerInstances);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                RaftNode raftNode = getRaftNode(followerInstances[0], METADATA_RAFT);
                assertEquals(getLeaderEndpoint(leader), getLeaderEndpoint(raftNode));
            }
        });

        RaftNode newLeader = getLeaderNode(METADATA_RAFT);

        // the new leader overwrites f2 with the new entry f3 on the same log index
        Future f3 = newLeader.replicate(new RaftAddOperation("val3"));

        assertEquals("val3", f3.get());
        try {
            f2.get();
            fail();
        } catch (Exception ignored) {
        }
    }

    @Test
    public void testCrashedLeader_cannotRecoverAndRejoinRaftGroup() throws Exception {
        raftAddresses = createRaftAddresses(2);
        instances = newInstances(raftAddresses);

        RaftNode leader = getLeaderNode(METADATA_RAFT);

        HazelcastInstance leaderInstance = getLeaderInstance(leader);
        final HazelcastInstance followerInstance = getRandomFollowerInstance(leader);

        leaderInstance.shutdown();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                RaftNode raftNode = getRaftNode(followerInstance, METADATA_RAFT);
                assertEquals(RaftRole.CANDIDATE, getRole(raftNode));
            }
        });

        final HazelcastInstance newInstance = factory.newHazelcastInstance(leader.getLocalEndpoint().getAddress(),
                createConfig(raftAddresses));
        assertClusterSizeEventually(2, followerInstance);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                RaftNode raftNode = getRaftNode(followerInstance, METADATA_RAFT);
                assertEquals(RaftRole.CANDIDATE, getRole(raftNode));

                raftNode = getRaftNode(newInstance, METADATA_RAFT);
                assertEquals(RaftRole.CANDIDATE, getRole(raftNode));
            }
        }, 10);
    }

    @Test
    public void testCrashedFollower_cannotRecoverAndRejoinRaftGroup() throws Exception {
        raftAddresses = createRaftAddresses(2);
        instances = newInstances(raftAddresses);

        final RaftNode leader = getLeaderNode(METADATA_RAFT);

        final HazelcastInstance leaderInstance = getLeaderInstance(leader);
        HazelcastInstance followerInstance = getRandomFollowerInstance(leader);

        Address restartingAddress = getAddress(followerInstance);
        followerInstance.shutdown();

        final HazelcastInstance newInstance = factory.newHazelcastInstance(restartingAddress, createConfig(raftAddresses));
        assertClusterSize(2, leaderInstance);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                RaftNode raftNode = getRaftNode(leaderInstance, METADATA_RAFT);
                assertEquals(RaftRole.LEADER, getRole(raftNode));

                raftNode = getRaftNode(newInstance, METADATA_RAFT);
                assertEquals(RaftRole.CANDIDATE, getRole(raftNode));
            }
        }, 10);
    }

    private RaftDataService getRaftTestService(HazelcastInstance instance) {
        return getNodeEngineImpl(instance).getService(RaftDataService.SERVICE_NAME);
    }

    private void assertLeaderNotEqualsEventually(final RaftNode leader, final HazelcastInstance... instances) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (HazelcastInstance instance : instances) {
                    RaftNode raftNode = getRaftNode(instance, METADATA_RAFT);
                    RaftEndpoint leaderEndpoint = getLeaderEndpoint(raftNode);
                    assertNotNull(leaderEndpoint);
                    assertNotEquals(leader.getLocalEndpoint(), leaderEndpoint);
                }
            }
        });
    }

    private HazelcastInstance getLeaderInstance(RaftNode leader) {
        Address address = leader.getLocalEndpoint().getAddress();
        for (HazelcastInstance instance : instances) {
            if (getAddress(instance).equals(address)) {
                return instance;
            }
        }
        throw new AssertionError("Cannot find leader instance!");
    }

    private HazelcastInstance getRandomFollowerInstance(RaftNode leader) {
        Address address = leader.getLocalEndpoint().getAddress();
        for (HazelcastInstance instance : instances) {
            if (!getAddress(instance).equals(address)) {
                return instance;
            }
        }
        throw new AssertionError("Cannot find non-leader instance!");
    }

    private HazelcastInstance[] getAllInstancesExcept(HazelcastInstance instance) {
        HazelcastInstance[] newInstances = new HazelcastInstance[instances.length - 1];
        int k = 0;
        for (HazelcastInstance hz : instances) {
            if (instance != hz) {
                newInstances[k++] = hz;
            }
        }
        return newInstances;
    }

    private Address[] createRaftAddresses(int count) {
        Address[] addresses = new Address[count];
        for (int i = 0; i < count; i++) {
            Address address = factory.nextAddress();
            addresses[i] = address;
        }
        return addresses;
    }

    private HazelcastInstance[] newInstances(Address[] addresses) {
        Config config = createConfig(addresses);

        int count = addresses.length;
        HazelcastInstance[] instances = new HazelcastInstance[count];
        for (int i = 0; i < count; i++) {
            instances[i] = factory.newHazelcastInstance(addresses[i], config);
        }
        assertClusterSizeEventually(count, instances);

        return instances;
    }

    private Config createConfig(Address[] addresses) {
        int count = addresses.length;
        RaftMember[] raftMembers = new RaftMember[count];
        for (int i = 0; i < count; i++) {
            Address addr = addresses[i];
            // assuming IPv4
            String address = addr.getHost() + ":" + addr.getPort();
            raftMembers[i] = new RaftMember(address, UuidUtil.newUnsecureUuidString());
        }

        Config config = new Config();
        configureSplitBrainDelay(config);

        RaftConfig raftConfig = new RaftConfig().setMembers(asList(raftMembers));
        ServiceConfig raftServiceConfig = new ServiceConfig().setEnabled(true).setName(RaftService.SERVICE_NAME)
                .setClassName(RaftService.class.getName()).setConfigObject(raftConfig);
        config.getServicesConfig().addServiceConfig(raftServiceConfig);

        ServiceConfig raftTestServiceConfig = new ServiceConfig().setEnabled(true)
                                                                 .setName(RaftDataService.SERVICE_NAME)
                                                                 .setClassName(RaftDataService.class.getName());
        config.getServicesConfig().addServiceConfig(raftTestServiceConfig);
        return config;
    }

    private void configureSplitBrainDelay(Config config) {
        config.setProperty(MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "15")
              .setProperty(MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "5");
    }

    private void block(HazelcastInstance leader, HazelcastInstance[] followers) {
        for (HazelcastInstance follower : followers) {
            blockCommunicationBetween(leader, follower);
        }

        for (HazelcastInstance follower : followers) {
            closeConnectionBetween(leader, follower);
        }

        assertClusterSizeEventually(2, followers);
        assertClusterSizeEventually(1, leader);
    }

    private void unblock(HazelcastInstance leader, HazelcastInstance[] followers) {
        for (HazelcastInstance follower : followers) {
            unblockCommunicationBetween(leader, follower);
        }
    }

    private RaftNode getLeaderNode(final String raftName) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertNotNull(getRaftNode(instances[0], raftName));
            }
        });

        RaftNode raftNode = getRaftNode(instances[0], raftName);
        waitUntilLeaderElected(raftNode);
        RaftEndpoint leaderEndpoint = getLeaderEndpoint(raftNode);

        for (HazelcastInstance instance : instances) {
            if (getAddress(instance).equals(leaderEndpoint.getAddress())) {
                return getRaftNode(instance, raftName);
            }
        }

        throw new IllegalStateException();
    }

}
