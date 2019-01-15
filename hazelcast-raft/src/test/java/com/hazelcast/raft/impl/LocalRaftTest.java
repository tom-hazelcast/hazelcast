package com.hazelcast.raft.impl;

import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.exception.CannotReplicateException;
import com.hazelcast.raft.exception.LeaderDemotedException;
import com.hazelcast.raft.exception.NotLeaderException;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.service.RaftApplyOperation;
import com.hazelcast.raft.impl.service.RaftDataService;
import com.hazelcast.raft.impl.testing.LocalRaftGroup;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.raft.impl.RaftUtil.getCommitIndex;
import static com.hazelcast.raft.impl.RaftUtil.getLastLogOrSnapshotEntry;
import static com.hazelcast.raft.impl.RaftUtil.getLeaderEndpoint;
import static com.hazelcast.raft.impl.RaftUtil.getTerm;
import static com.hazelcast.raft.impl.RaftUtil.newGroupWithService;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LocalRaftTest extends HazelcastTestSupport {

    private LocalRaftGroup group;

    @Before
    public void init() {
    }

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test
    public void when_twoNodeCluster_then_leaderIsElected() throws Exception {
        testLeaderElection(2);
    }

    @Test
    public void when_threeNodeCluster_then_leaderIsElected() throws Exception {
        testLeaderElection(3);
    }

    private void testLeaderElection(int nodeCount) throws Exception {
        group = new LocalRaftGroup(nodeCount, new RaftConfig());
        group.start();
        group.waitUntilLeaderElected();

        RaftEndpoint leaderEndpoint = group.getLeaderEndpoint();
        assertNotNull(leaderEndpoint);

        int leaderIndex = group.getLeaderIndex();
        assertThat(leaderIndex, greaterThanOrEqualTo(0));

        RaftNode leaderNode = group.getLeaderNode();
        assertNotNull(leaderNode);
    }

    @Test
    public void when_twoNodeCluster_then_singleEntryCommitted() throws Exception {
        testSingleCommitEntry(2);
    }

    @Test
    public void when_threeNodeCluster_then_singleEntryCommitted() throws Exception {
        testSingleCommitEntry(3);
    }

    private void testSingleCommitEntry(final int nodeCount) throws Exception {
        group = newGroupWithService(nodeCount, new RaftConfig());
        group.start();
        group.waitUntilLeaderElected();

        final Object val = "val";
        Object result = group.getLeaderNode().replicate(new RaftApplyOperation(val)).get();
        assertEquals(result, val);

        final int commitIndex = 1;
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (int i = 0; i < nodeCount; i++) {
                    RaftNode node = group.getNode(i);
                    int index = getCommitIndex(node);
                    System.err.println("index = " + index);
                    assertEquals(commitIndex, index);
                    RaftDataService service = group.getIntegration(i).getService();
                    Object actual = service.get(commitIndex);
                    System.err.println("actual = " + actual);
                    assertEquals(val, actual);
                }
            }
        });
    }

    @Test(expected = NotLeaderException.class)
    public void when_followerAttemptsToReplicate_then_itFails() throws ExecutionException, InterruptedException {
        group = newGroupWithService(3, new RaftConfig());
        group.start();
        RaftNode leader = group.waitUntilLeaderElected();
        RaftNode[] followers = group.getNodesExcept(leader.getLocalEndpoint());

        followers[0].replicate(new RaftApplyOperation("val")).get();

        for (RaftNode raftNode : group.getNodes()) {
            RaftDataService service = group.getIntegration(raftNode.getLocalEndpoint()).getService();
            assertEquals(0, service.size());
        }
    }

    @Test
    public void when_twoNodeCluster_then_leaderCannotCommitWithOnlyLocalAppend() throws ExecutionException, InterruptedException {
        testNoCommitWhenOnlyLeaderAppends(2);
    }

    @Test
    public void when_threeNodeCluster_then_leaderCannotCommitWithOnlyLocalAppend() throws ExecutionException, InterruptedException {
        testNoCommitWhenOnlyLeaderAppends(3);
    }

    private void testNoCommitWhenOnlyLeaderAppends(int nodeCount)
            throws InterruptedException, ExecutionException {
        group = newGroupWithService(nodeCount, new RaftConfig());
        group.start();
        RaftNode leader = group.waitUntilLeaderElected();

        group.dropMessagesToAll(leader.getLocalEndpoint(), AppendRequest.class);

        try {
            leader.replicate(new RaftApplyOperation("val")).get(10, TimeUnit.SECONDS);
            fail();
        } catch (TimeoutException ignored) {
        }

        for (RaftNode raftNode : group.getNodes()) {
            assertEquals(0, getCommitIndex(raftNode));
            RaftDataService service = group.getIntegration(raftNode.getLocalEndpoint()).getService();
            assertEquals(0, service.size());
        }
    }

    @Test
    public void when_leaderAppendsToMinority_then_itCannotCommit() throws ExecutionException, InterruptedException {
        group = newGroupWithService(5, new RaftConfig());
        group.start();
        final RaftNode leader = group.waitUntilLeaderElected();
        final RaftNode[] followers = group.getNodesExcept(leader.getLocalEndpoint());

        for (int i = 1; i < followers.length; i++) {
            group.dropMessagesToEndpoint(leader.getLocalEndpoint(), followers[i].getLocalEndpoint(), AppendRequest.class);
        }

        Future f = leader.replicate(new RaftApplyOperation("val"));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, getLastLogOrSnapshotEntry(leader).index());
                assertEquals(1, getLastLogOrSnapshotEntry(followers[0]).index());
            }
        });

        try {
            f.get(10, TimeUnit.SECONDS);
            fail();
        } catch (TimeoutException ignored) {
        }

        for (RaftNode raftNode : group.getNodes()) {
            assertEquals(0, getCommitIndex(raftNode));
            RaftDataService service = group.getIntegration(raftNode.getLocalEndpoint()).getService();
            assertEquals(0, service.size());
        }
    }

    @Test
    public void when_fourNodeCluster_then_leaderReplicateEntriesSequentially() throws ExecutionException, InterruptedException {
        testReplicateEntriesSequentially(4);
    }

    @Test
    public void when_fiveNodeCluster_then_leaderReplicateEntriesSequentially() throws ExecutionException, InterruptedException {
        testReplicateEntriesSequentially(5);
    }


    private void testReplicateEntriesSequentially(int nodeCount) throws ExecutionException, InterruptedException {
        group = newGroupWithService(nodeCount, newRaftConfigWithNoSnapshotting());
        group.start();
        RaftNode leader = group.waitUntilLeaderElected();

        final int entryCount = 100;
        for (int i = 0; i < entryCount; i++) {
            final Object val = "val" + i;
            leader.replicate(new RaftApplyOperation(val)).get();
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (RaftNode raftNode : group.getNodes()) {
                    assertEquals(entryCount, getCommitIndex(raftNode));
                    RaftDataService service = group.getService(raftNode);
                    assertEquals(100, service.size());
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
    public void when_fourNodeCluster_then_leaderReplicatesEntriesConcurrently() throws ExecutionException, InterruptedException {
        testReplicateEntriesConcurrently(4);
    }

    @Test
    public void when_fiveNodeCluster_then_leaderReplicatesEntriesConcurrently() throws ExecutionException, InterruptedException {
        testReplicateEntriesConcurrently(5);
    }

    private void testReplicateEntriesConcurrently(int nodeCount) throws ExecutionException, InterruptedException {
        group = newGroupWithService(nodeCount, newRaftConfigWithNoSnapshotting());
        group.start();
        RaftNode leader = group.waitUntilLeaderElected();

        final int entryCount = 100;
        List<Future> futures = new ArrayList<Future>(entryCount);
        for (int i = 0; i < entryCount; i++) {
            final Object val = "val" + i;
            futures.add(leader.replicate(new RaftApplyOperation(val)));
        }

        for (Future f : futures) {
            f.get();
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (RaftNode raftNode : group.getNodes()) {
                    assertEquals(entryCount, getCommitIndex(raftNode));
                    RaftDataService service = group.getService(raftNode);
                    assertEquals(100, service.size());
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
    public void when_fourNodeCluster_then_entriesAreSubmittedInParallel() throws ExecutionException, InterruptedException {
        testReplicateEntriesInParallel(4);
    }

    @Test
    public void when_fiveNodeCluster_then_entriesAreSubmittedInParallel() throws ExecutionException, InterruptedException {
        testReplicateEntriesInParallel(5);
    }

    private void testReplicateEntriesInParallel(int nodeCount) throws ExecutionException, InterruptedException {
        group = newGroupWithService(nodeCount, newRaftConfigWithNoSnapshotting());
        group.start();
        final RaftNode leader = group.waitUntilLeaderElected();

        int threadCount = 10;
        final int opsPerThread = 10;
        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            final int start = i * opsPerThread;
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    final List<Future> futures = new ArrayList<Future>();
                    for (int j = start; j < start + opsPerThread; j++) {
                        futures.add(leader.replicate(new RaftApplyOperation(j)));
                    }

                    for (Future f : futures) {
                        try {
                            f.get();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (ExecutionException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        final int entryCount = threadCount * opsPerThread;

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (RaftNode raftNode : group.getNodes()) {
                    assertEquals(entryCount, getCommitIndex(raftNode));
                    RaftDataService service = group.getService(raftNode);
                    assertEquals(entryCount, service.size());
                    Set<Object> values = service.values();
                    for (int i = 0; i < entryCount; i++) {
                        assertTrue(values.contains(i));
                    }
                }
            }
        });
    }

    @Test
    public void when_followerSlowsDown_then_itCatchesLeaderEventually() throws ExecutionException, InterruptedException {
        group = newGroupWithService(3, newRaftConfigWithNoSnapshotting());
        group.start();
        RaftNode leader = group.waitUntilLeaderElected();
        RaftNode slowFollower = group.getAnyFollowerNode();

        group.dropMessagesToEndpoint(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendRequest.class);

        final int entryCount = 100;
        for (int i = 0; i < entryCount; i++) {
            final Object val = "val" + i;
            leader.replicate(new RaftApplyOperation(val)).get();
        }

        assertEquals(0, getCommitIndex(slowFollower));

        group.resetAllDropRulesFrom(leader.getLocalEndpoint());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (RaftNode raftNode : group.getNodes()) {
                    assertEquals(entryCount, getCommitIndex(raftNode));
                    RaftDataService service = group.getService(raftNode);
                    assertEquals(entryCount, service.size());
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
    public void when_disruptiveFollowerStartsElection_then_itCannotTakeOverLeadershipFromLegitimateLeader()
            throws ExecutionException, InterruptedException {
        group = newGroupWithService(3, new RaftConfig());
        group.start();
        RaftNode leader = group.waitUntilLeaderElected();
        final int leaderTerm = getTerm(leader);
        final RaftNode disruptiveFollower = group.getAnyFollowerNode();

        group.dropMessagesToEndpoint(leader.getLocalEndpoint(), disruptiveFollower.getLocalEndpoint(), AppendRequest.class);

        leader.replicate(new RaftApplyOperation("val")).get();

        group.split(disruptiveFollower.getLocalEndpoint());

        final int[] disruptiveFollowerTermRef = new int[1];
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                int followerTerm = getTerm(disruptiveFollower);
                assertEquals(leaderTerm, followerTerm);
                disruptiveFollowerTermRef[0] = followerTerm;
            }
        }, 5);

        group.resetAllDropRulesFrom(leader.getLocalEndpoint());
        group.merge();

        RaftNode newLeader = group.waitUntilLeaderElected();
        assertNotEquals(disruptiveFollower.getLocalEndpoint(), newLeader.getLocalEndpoint());
        assertEquals(getTerm(newLeader), disruptiveFollowerTermRef[0]);
    }


    @Test
    public void when_followerTerminatesInMinority_then_clusterRemainsAvailable() throws Exception {
        group = newGroupWithService(3, new RaftConfig());
        group.start();
        RaftNode leaderNode = group.waitUntilLeaderElected();

        int leaderIndex = group.getLeaderIndex();
        for (int i = 0; i < group.size(); i++) {
            if (i != leaderIndex) {
                group.terminateNode(i);
                break;
            }
        }

        String value = "value";
        Future future = leaderNode.replicate(new RaftApplyOperation(value));
        assertEquals(value, future.get());
    }

    @Test
    public void when_leaderTerminatesInMinority_then_clusterRemainsAvailable() throws Exception {
        group = newGroupWithService(3, new RaftConfig());
        group.start();

        final RaftNode leaderNode = group.waitUntilLeaderElected();
        final RaftNode[] followers = group.getNodesExcept(leaderNode.getLocalEndpoint());
        int leaderTerm = getTerm(leaderNode);

        group.terminateNode(leaderNode.getLocalEndpoint());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (RaftNode raftNode : followers) {
                    assertNotEquals(leaderNode.getLocalEndpoint(), getLeaderEndpoint(raftNode));
                }
            }
        });

        RaftNode newLeader = group.waitUntilLeaderElected();
        assertTrue(getTerm(newLeader) > leaderTerm);

        String value = "value";
        Future future = newLeader.replicate(new RaftApplyOperation(value));
        assertEquals(value, future.get());
    }

    @Test
    public void when_leaderStaysInMajorityDuringSplit_thenItMergesBackSuccessfully() throws Exception {
        group = new LocalRaftGroup(5);
        group.start();
        group.waitUntilLeaderElected();

        final int[] split = group.createMinoritySplitIndexes(false);
        group.split(split);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (int ix : split) {
                    assertNull(getLeaderEndpoint(group.getNode(ix)));
                }
            }
        });

        group.merge();
        group.waitUntilLeaderElected();
    }

    @Test
    public void when_leaderStaysInMinorityDuringSplit_thenItMergesBackSuccessfully() throws Exception {
        int nodeCount = 5;
        group = new LocalRaftGroup(nodeCount);
        group.start();
        final RaftEndpoint leaderEndpoint = group.waitUntilLeaderElected().getLocalEndpoint();

        final int[] split = group.createMajoritySplitIndexes(false);
        group.split(split);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (int ix : split) {
                    assertNotEquals(leaderEndpoint, getLeaderEndpoint(group.getNode(ix)));
                }
            }
        });

        for (int i = 0; i < nodeCount; i++) {
            if (Arrays.binarySearch(split, i) < 0) {
                assertEquals(leaderEndpoint, getLeaderEndpoint(group.getNode(i)));
            }
        }

        group.merge();
        group.waitUntilLeaderElected();
    }

    @Test
    public void when_leaderCrashes_then_theFollowerWithLongestLogBecomesLeader() throws Exception {
        group = newGroupWithService(4, new RaftConfig());
        group.start();

        RaftNode leader = group.waitUntilLeaderElected();

        leader.replicate(new RaftApplyOperation("val1")).get();

        final RaftNode[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        final RaftNode nextLeader = followers[0];
        final int commitIndex = getCommitIndex(leader);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (RaftNode raftNode : group.getNodes()) {
                    assertEquals(commitIndex, getCommitIndex(raftNode));
                }
            }
        });

        for (int i = 1; i < followers.length; i++) {
            group.dropMessagesToEndpoint(leader.getLocalEndpoint(), followers[i].getLocalEndpoint(), AppendRequest.class);
        }

        leader.replicate(new RaftApplyOperation("val2"));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(getLastLogOrSnapshotEntry(nextLeader).index() > commitIndex);
            }
        });

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (RaftNode raftNode : group.getNodes()) {
                    assertEquals(commitIndex, getCommitIndex(raftNode));
                }
            }
        }, 10);

        group.terminateNode(leader.getLocalEndpoint());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (RaftNode raftNode : followers) {
                    assertEquals(nextLeader.getLocalEndpoint(), getLeaderEndpoint(raftNode));
                }
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (RaftNode raftNode : followers) {
                    assertEquals(commitIndex, getCommitIndex(raftNode));
                    assertTrue(getLastLogOrSnapshotEntry(raftNode).index() > commitIndex);
                    RaftDataService service = group.getService(raftNode);
                    assertEquals(1, service.size());
                    assertEquals("val1", service.get(1));
                    // val2 not committed yet
                }
            }
        });
    }

    @Test
    public void when_followerBecomesLeaderWithUncommittedEntries_then_thoseEntriesAreCommittedWithANewEntryOfCurrentTerm() throws Exception {
        group = newGroupWithService(3, new RaftConfig());
        group.start();

        final RaftNode leader = group.waitUntilLeaderElected();

        leader.replicate(new RaftApplyOperation("val1")).get();

        final RaftNode[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        final RaftNode nextLeader = followers[0];
        final int commitIndex = getCommitIndex(leader);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (RaftNode raftNode : group.getNodes()) {
                    assertEquals(commitIndex, getCommitIndex(raftNode));
                }
            }
        });

        group.dropMessagesToEndpoint(leader.getLocalEndpoint(), followers[1].getLocalEndpoint(), AppendRequest.class);
        group.dropMessagesToEndpoint(nextLeader.getLocalEndpoint(), leader.getLocalEndpoint(), AppendSuccessResponse.class);

        leader.replicate(new RaftApplyOperation("val2"));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(getLastLogOrSnapshotEntry(nextLeader).index() > commitIndex);
            }
        });

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (RaftNode raftNode : group.getNodes()) {
                    assertEquals(commitIndex, getCommitIndex(raftNode));
                }
            }
        }, 10);

        group.terminateNode(leader.getLocalEndpoint());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (RaftNode raftNode : followers) {
                    assertEquals(nextLeader.getLocalEndpoint(), getLeaderEndpoint(raftNode));
                }
            }
        });

        nextLeader.replicate(new RaftApplyOperation("val3"));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (RaftNode raftNode : followers) {
                    assertEquals(3, getCommitIndex(raftNode));
                    assertEquals(3, getLastLogOrSnapshotEntry(raftNode).index());
                    RaftDataService service = group.getService(raftNode);
                    assertEquals(3, service.size());
                    assertEquals("val1", service.get(1));
                    assertEquals("val2", service.get(2));
                    assertEquals("val3", service.get(3));
                }
            }
        });
    }

    @Test
    public void when_leaderCrashes_then_theFollowerWithLongestLogMayNotBecomeLeaderIfItsLogIsNotMajority() throws Exception {
        group = newGroupWithService(5, new RaftConfig());
        group.start();

        final RaftNode leader = group.waitUntilLeaderElected();

        leader.replicate(new RaftApplyOperation("val1")).get();

        final RaftNode[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        final RaftNode followerWithLongerLog = followers[0];
        final int commitIndex = getCommitIndex(leader);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (RaftNode raftNode : group.getNodes()) {
                    assertEquals(commitIndex, getCommitIndex(raftNode));
                }
            }
        });

        for (int i = 1; i < followers.length; i++) {
            group.dropMessagesToEndpoint(leader.getLocalEndpoint(), followers[i].getLocalEndpoint(), AppendRequest.class);
        }

        leader.replicate(new RaftApplyOperation("val2"));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(getLastLogOrSnapshotEntry(followerWithLongerLog).index() > commitIndex);
            }
        });

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (RaftNode raftNode : group.getNodes()) {
                    assertEquals(commitIndex, getCommitIndex(raftNode));
                }
            }
        }, 10);

        group.dropMessagesToEndpoint(followerWithLongerLog.getLocalEndpoint(), followers[1].getLocalEndpoint(), VoteRequest.class);
        group.dropMessagesToEndpoint(followerWithLongerLog.getLocalEndpoint(), followers[2].getLocalEndpoint(), VoteRequest.class);

        group.terminateNode(leader.getLocalEndpoint());

        RaftNode newLeader = group.waitUntilLeaderElected();

        // followerWithLongerLog has 2 entries, other 3 followers have 1 entry and the 3 followers will elect their leaders

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (RaftNode raftNode : followers) {
                    RaftEndpoint newLeader = getLeaderEndpoint(raftNode);
                    assertNotEquals(leader.getLocalEndpoint(), newLeader);
                    assertNotEquals(followerWithLongerLog.getLocalEndpoint(), newLeader);
                }
            }
        });

        for (int i = 1; i < followers.length; i++) {
            assertEquals(commitIndex, getCommitIndex(followers[i]));
            assertEquals(commitIndex, getLastLogOrSnapshotEntry(followers[i]).index());
        }

        // followerWithLongerLog has not truncated its extra log entry until the new leader appends a new entry
        assertTrue(getLastLogOrSnapshotEntry(followerWithLongerLog).index() > commitIndex);

        newLeader.replicate(new RaftApplyOperation("val3")).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (RaftNode raftNode : followers) {
                    assertEquals(2, getCommitIndex(raftNode));
                    RaftDataService service = group.getService(raftNode);
                    assertEquals(2, service.size());
                    assertEquals("val1", service.get(1));
                    assertEquals("val3", service.get(2));
                }
            }
        });

        assertEquals(2, getLastLogOrSnapshotEntry(followerWithLongerLog).index());
    }

    @Test
    public void when_leaderStaysInMinorityDuringSplit_then_itCannotCommitNewEntries() throws ExecutionException, InterruptedException {
        group = newGroupWithService(3, newRaftConfigWithNoSnapshotting());
        group.start();

        final RaftNode leader = group.waitUntilLeaderElected();

        leader.replicate(new RaftApplyOperation("val1")).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (RaftNode raftNode : group.getNodes()) {
                    assertEquals(1, getCommitIndex(raftNode));
                }
            }
        });

        final RaftNode[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        group.split(leader.getLocalEndpoint());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (RaftNode raftNode : followers) {
                    RaftEndpoint leaderEndpoint = getLeaderEndpoint(raftNode);
                    assertNotNull(leaderEndpoint);
                    assertNotEquals(leader.getLocalEndpoint(), leaderEndpoint);
                }
            }
        });

        List<Future> isolatedFutures = new ArrayList<Future>();
        for (int i = 0; i < 10; i++) {
            isolatedFutures.add(leader.replicate(new RaftApplyOperation("isolated" + i)));
        }

        RaftNode newLeader = group.getNode(getLeaderEndpoint(followers[0]));
        for (int i = 0; i < 10; i++) {
            newLeader.replicate(new RaftApplyOperation("valNew" + i)).get();
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (RaftNode raftNode : followers) {
                    assertEquals(11, getCommitIndex(raftNode));
                }
            }
        });

        group.merge();

        RaftNode finalLeader = group.waitUntilLeaderElected();

        assertNotEquals(leader.getLocalEndpoint(), finalLeader.getLocalEndpoint());
        for (Future f : isolatedFutures) {
            try {
                f.get();
                fail();
            } catch (LeaderDemotedException ignored) {
            }
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (RaftNode raftNode : followers) {
                    RaftDataService service = group.getService(raftNode);
                    assertEquals(11, service.size());
                    assertEquals("val1", service.get(1));
                    for (int i = 0; i < 10; i++) {
                        assertEquals(("valNew" + i), service.get(i + 2));
                    }
                }
            }
        });
    }

    @Test
    public void when_thereAreTooManyInflightAppendedEntries_then_newAppendsAreRejected() throws ExecutionException, InterruptedException {
        int uncommittedEntryCount = 10;
        RaftConfig raftConfig = new RaftConfig().setUncommittedEntryCountToRejectNewAppends(uncommittedEntryCount);
        group = newGroupWithService(2, raftConfig);
        group.start();

        RaftNode leader = group.waitUntilLeaderElected();
        RaftNode follower = group.getAnyFollowerNode();
        group.terminateNode(follower.getLocalEndpoint());

        for (int i = 0; i < uncommittedEntryCount; i++) {
            leader.replicate(new RaftApplyOperation("val" + i));
        }

        try {
            leader.replicate(new RaftApplyOperation("valFinal")).get();
            fail();
        } catch (CannotReplicateException ignored) {
        }
    }

    private static RaftConfig newRaftConfigWithNoSnapshotting() {
        return new RaftConfig().setCommitIndexAdvanceCountToSnapshot(Integer.MAX_VALUE);
    }
}
