package com.hazelcast.raft.impl;

import com.hazelcast.raft.MembershipChangeType;
import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.exception.CannotReplicateException;
import com.hazelcast.raft.exception.MemberAlreadyExistsException;
import com.hazelcast.raft.exception.MemberDoesNotExistException;
import com.hazelcast.raft.impl.RaftNode.RaftNodeStatus;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.raft.impl.service.RaftTestApplyOperation;
import com.hazelcast.raft.impl.service.RaftDataService;
import com.hazelcast.raft.impl.state.RaftGroupMembers;
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

import java.util.concurrent.ExecutionException;

import static com.hazelcast.raft.MembershipChangeType.REMOVE;
import static com.hazelcast.raft.impl.RaftUtil.getCommitIndex;
import static com.hazelcast.raft.impl.RaftUtil.getCommittedGroupMembers;
import static com.hazelcast.raft.impl.RaftUtil.getLastGroupMembers;
import static com.hazelcast.raft.impl.RaftUtil.getLastLogOrSnapshotEntry;
import static com.hazelcast.raft.impl.RaftUtil.getLeaderEndpoint;
import static com.hazelcast.raft.impl.RaftUtil.getSnapshotEntry;
import static com.hazelcast.raft.impl.RaftUtil.getStatus;
import static com.hazelcast.raft.impl.RaftUtil.newGroupWithService;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MembershipChangeTest extends HazelcastTestSupport {

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
    public void when_newRaftNodeJoins_then_itAppendsMissingEntries() throws ExecutionException, InterruptedException {
        group = newGroupWithService(3, new RaftConfig());
        group.start();

        final RaftNode leader = group.waitUntilLeaderElected();
        leader.replicate(new RaftTestApplyOperation("val")).get();

        final RaftNode newRaftNode = group.createNewRaftNode();

        leader.replicateMembershipChange(newRaftNode.getLocalEndpoint(), MembershipChangeType.ADD).get();

        final int commitIndex = getCommitIndex(leader);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(commitIndex, getCommitIndex(newRaftNode));
            }
        });

        final RaftGroupMembers lastGroupMembers = RaftUtil.getLastGroupMembers(leader);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNode raftNode : group.getNodes()) {
                    assertEquals(RaftNodeStatus.ACTIVE, getStatus(raftNode));
                    assertEquals(lastGroupMembers.members(), getLastGroupMembers(raftNode).members());
                    assertEquals(lastGroupMembers.index(), getLastGroupMembers(raftNode).index());
                    assertEquals(lastGroupMembers.members(), getCommittedGroupMembers(raftNode).members());
                    assertEquals(lastGroupMembers.index(), getCommittedGroupMembers(raftNode).index());
                }
            }
        });

        RaftDataService service = group.getService(newRaftNode);
        assertEquals(1, service.size());
        assertTrue(service.values().contains("val"));
    }

    @Test
    public void when_followerLeaves_then_itIsRemovedFromTheGroupMembers() throws ExecutionException, InterruptedException {
        group = newGroupWithService(3, new RaftConfig());
        group.start();

        final RaftNode leader = group.waitUntilLeaderElected();
        RaftNode[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        final RaftNode leavingFollower = followers[0];
        final RaftNode stayingFollower = followers[1];

        leader.replicate(new RaftTestApplyOperation("val")).get();

        leader.replicateMembershipChange(leavingFollower.getLocalEndpoint(), REMOVE).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNode raftNode : asList(leader, stayingFollower)) {
                    assertFalse(getLastGroupMembers(raftNode).isKnownEndpoint(leavingFollower.getLocalEndpoint()));
                    assertFalse(getCommittedGroupMembers(raftNode).isKnownEndpoint(leavingFollower.getLocalEndpoint()));
                }
            }
        });

        group.terminateNode(leavingFollower.getLocalEndpoint());
    }

    @Test
    public void when_leaderLeaves_then_itIsRemovedFromTheGroupMembers() throws ExecutionException, InterruptedException {
        group = newGroupWithService(3, new RaftConfig());
        group.start();

        final RaftNode leader = group.waitUntilLeaderElected();
        final RaftNode[] followers = group.getNodesExcept(leader.getLocalEndpoint());

        leader.replicate(new RaftTestApplyOperation("val")).get();
        leader.replicateMembershipChange(leader.getLocalEndpoint(), REMOVE).get();

        assertEquals(RaftNodeStatus.STEPPED_DOWN, getStatus(leader));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNode raftNode : followers) {
                    assertFalse(getLastGroupMembers(raftNode).isKnownEndpoint(leader.getLocalEndpoint()));
                    assertFalse(getCommittedGroupMembers(raftNode).isKnownEndpoint(leader.getLocalEndpoint()));
                }
            }
        });
    }

    @Test
    public void when_leaderLeaves_then_itCannotVoteForCommitOfMemberChange() throws ExecutionException, InterruptedException {
        group = newGroupWithService(3, new RaftConfig().setLeaderHeartbeatPeriodInMillis(1000));
        group.start();

        final RaftNode leader = group.waitUntilLeaderElected();
        RaftNode[] followers = group.getNodesExcept(leader.getLocalEndpoint());

        group.dropMessagesToEndpoint(followers[0].getLocalEndpoint(), leader.getLocalEndpoint(), AppendSuccessResponse.class);
        leader.replicate(new RaftTestApplyOperation("val")).get();

        leader.replicateMembershipChange(leader.getLocalEndpoint(), MembershipChangeType.REMOVE);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, getCommitIndex(leader));
            }
        }, 10);
    }

    @Test
    public void when_leaderLeaves_then_followersElectNewLeader() throws ExecutionException, InterruptedException {
        group = newGroupWithService(3, new RaftConfig());
        group.start();

        final RaftNode leader = group.waitUntilLeaderElected();
        final RaftNode[] followers = group.getNodesExcept(leader.getLocalEndpoint());

        leader.replicate(new RaftTestApplyOperation("val")).get();
        leader.replicateMembershipChange(leader.getLocalEndpoint(), REMOVE).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNode raftNode : followers) {
                    assertFalse(getLastGroupMembers(raftNode).isKnownEndpoint(leader.getLocalEndpoint()));
                    assertFalse(getCommittedGroupMembers(raftNode).isKnownEndpoint(leader.getLocalEndpoint()));
                }
            }
        });

        group.terminateNode(leader.getLocalEndpoint());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNode raftNode : followers) {
                    assertNotEquals(leader.getLocalEndpoint(), getLeaderEndpoint(raftNode));
                }
            }
        });
    }

    @Test
    public void when_membershipChangeRequestIsMadeWithWrongType_then_theChangeFails() throws ExecutionException, InterruptedException {
        group = newGroupWithService(3, new RaftConfig());
        group.start();

        final RaftNode leader = group.waitUntilLeaderElected();
        leader.replicate(new RaftTestApplyOperation("val")).get();

        try {
            leader.replicateMembershipChange(leader.getLocalEndpoint(), null).get();
            fail();
        } catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    public void when_nonExistingEndpointIsRemoved_then_theChangeFails() throws ExecutionException, InterruptedException {
        group = newGroupWithService(3, new RaftConfig());
        group.start();

        final RaftNode leader = group.waitUntilLeaderElected();
        final RaftNode leavingFollower = group.getAnyFollowerNode();

        leader.replicate(new RaftTestApplyOperation("val")).get();
        leader.replicateMembershipChange(leavingFollower.getLocalEndpoint(), MembershipChangeType.REMOVE).get();

        try {
            leader.replicateMembershipChange(leavingFollower.getLocalEndpoint(), MembershipChangeType.REMOVE).get();
            fail();
        } catch (MemberDoesNotExistException ignored) {
        }
    }

    @Test
    public void when_existingEndpointIsAdded_then_theChangeFails() throws ExecutionException, InterruptedException {
        group = newGroupWithService(3, new RaftConfig());
        group.start();

        final RaftNode leader = group.waitUntilLeaderElected();

        leader.replicate(new RaftTestApplyOperation("val")).get();

        try {
            leader.replicateMembershipChange(leader.getLocalEndpoint(), MembershipChangeType.ADD).get();
            fail();
        } catch (MemberAlreadyExistsException ignored) {
        }
    }

    @Test
    public void when_thereIsNoCommitInTheCurrentTerm_then_cannotMakeMemberChange() throws ExecutionException, InterruptedException {
        // https://groups.google.com/forum/#!msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J

        group = newGroupWithService(3, new RaftConfig());
        group.start();

        final RaftNode leader = group.waitUntilLeaderElected();

        try {
            leader.replicateMembershipChange(leader.getLocalEndpoint(), MembershipChangeType.REMOVE).get();
            fail();
        } catch (CannotReplicateException ignored) {
        }
    }

    @Test
    public void when_appendNopEntryOnLeaderElection_then_canMakeMemberChangeAfterNopEntryCommitted() {
        // https://groups.google.com/forum/#!msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J

        group = newGroupWithService(3, new RaftConfig().setAppendNopEntryOnLeaderElection(true));
        group.start();

        final RaftNode leader = group.waitUntilLeaderElected();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                // may fail until nop-entry is committed
                try {
                    leader.replicateMembershipChange(leader.getLocalEndpoint(), MembershipChangeType.REMOVE).get();
                } catch (CannotReplicateException e) {
                    fail(e.getMessage());
                }
            }
        });
    }

    @Test
    public void when_newJoiningNodeFirstReceivesSnapshot_then_itInstallsSnapshot() throws ExecutionException, InterruptedException {
        group = newGroupWithService(3, new RaftConfig().setCommitIndexAdvanceCountToSnapshot(5));
        group.start();

        final RaftNode leader = group.waitUntilLeaderElected();
        for (int i = 0; i < 4; i++) {
            leader.replicate(new RaftTestApplyOperation("val" + i)).get();
        }

        final RaftNode newRaftNode = group.createNewRaftNode();

        group.dropMessagesToEndpoint(leader.getLocalEndpoint(), newRaftNode.getLocalEndpoint(), AppendRequest.class);

        leader.replicateMembershipChange(newRaftNode.getLocalEndpoint(), MembershipChangeType.ADD).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(getSnapshotEntry(leader).index() > 0);
            }
        });

        group.resetAllDropRulesFrom(leader.getLocalEndpoint());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(getCommitIndex(leader), getCommitIndex(newRaftNode));
                assertEquals(getLastGroupMembers(leader).members(), getLastGroupMembers(newRaftNode).members());
                assertEquals(getLastGroupMembers(leader).members(), getCommittedGroupMembers(newRaftNode).members());
                RaftDataService service = group.getService(newRaftNode);
                assertEquals(4, service.size());
            }
        });
    }

    @Test
    public void when_leaderFailsWhileLeavingRaftGroup_othersCommitTheMemberChange() throws ExecutionException, InterruptedException {
        group = newGroupWithService(3, new RaftConfig());
        group.start();

        final RaftNode leader = group.waitUntilLeaderElected();
        final RaftNode[] followers = group.getNodesExcept(leader.getLocalEndpoint());

        leader.replicate(new RaftTestApplyOperation("val")).get();

        for (RaftNode follower : followers) {
            group.dropMessagesToEndpoint(follower.getLocalEndpoint(), leader.getLocalEndpoint(), AppendSuccessResponse.class);
            group.dropMessagesToEndpoint(follower.getLocalEndpoint(), leader.getLocalEndpoint(), AppendFailureResponse.class);
        }

        leader.replicateMembershipChange(leader.getLocalEndpoint(), MembershipChangeType.REMOVE);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNode follower : followers) {
                    assertEquals(2, getLastLogOrSnapshotEntry(follower).index());
                }
            }
        });

        group.terminateNode(leader.getLocalEndpoint());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNode follower : followers) {
                    final RaftEndpoint newLeaderEndpoint = getLeaderEndpoint(follower);
                    assertNotNull(newLeaderEndpoint);
                    assertNotEquals(leader.getLocalEndpoint(), newLeaderEndpoint);
                }
            }
        });

        final RaftNode newLeader = group.getNode(getLeaderEndpoint(followers[0]));
        newLeader.replicate(new RaftTestApplyOperation("val2"));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNode follower : followers) {
                    assertFalse(getCommittedGroupMembers(follower).isKnownEndpoint(leader.getLocalEndpoint()));
                }
            }
        });
    }
}
