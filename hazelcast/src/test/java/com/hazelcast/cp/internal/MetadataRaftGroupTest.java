/*
 *  Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;
import com.hazelcast.cp.CPGroup.CPGroupStatus;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raftop.metadata.CreateRaftGroupOp;
import com.hazelcast.cp.internal.raftop.metadata.GetActiveCPMembersOp;
import com.hazelcast.cp.internal.raftop.metadata.GetRaftGroupOp;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.ExceptionUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.cp.internal.raft.QueryPolicy.LEADER_LOCAL;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLeaderMember;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getSnapshotEntry;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.waitUntilLeaderElected;
import static com.hazelcast.cp.internal.MetadataRaftGroupManager.METADATA_GROUP_ID;
import static com.hazelcast.test.SplitBrainTestSupport.blockCommunicationBetween;
import static com.hazelcast.test.SplitBrainTestSupport.unblockCommunicationBetween;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MetadataRaftGroupTest extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;

    @Test
    public void when_clusterStartsWithNonCPNodes_then_metadataClusterIsInitialized() {
        int cpNodeCount = 3;
        instances = newInstances(cpNodeCount, cpNodeCount, 2);

        final List<Address> raftAddresses = new ArrayList<Address>();
        for (int i = 0; i < cpNodeCount; i++) {
            raftAddresses.add(getAddress(instances[i]));
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    if (raftAddresses.contains(getAddress(instance))) {
                        assertNotNull(getRaftNode(instance, METADATA_GROUP_ID));
                    }
                }
            }
        });

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    if (!raftAddresses.contains(getAddress(instance))) {
                        assertNull(getRaftNode(instance, METADATA_GROUP_ID));
                    }
                }
            }
        }, 10);
    }

    @Test
    public void when_raftGroupIsCreatedWithAllCPNodes_then_raftNodeIsCreatedOnAll()  {
        int nodeCount = 5;
        instances = newInstances(nodeCount);

        final CPGroupId groupId = createNewRaftGroup(instances[0], "id", nodeCount);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    assertNotNull(getRaftNode(instance, groupId));
                }
            }
        });
    }

    @Test
    public void when_raftGroupIsCreatedWithSomeCPNodes_then_raftNodeIsCreatedOnOnlyThem() {
        final int nodeCount = 5;
        final int metadataGroupSize = 3;
        instances = newInstances(nodeCount, metadataGroupSize, 0);

        final List<Member> raftMembers = new ArrayList<Member>();
        for (int i = 0; i < nodeCount; i++) {
            raftMembers.add(instances[i].getCluster().getLocalMember());
        }

        Collections.sort(raftMembers, new Comparator<Member>() {
            @Override
            public int compare(Member o1, Member o2) {
                return o1.getUuid().compareTo(o2.getUuid());
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (Member member : raftMembers.subList(0, metadataGroupSize)) {
                    HazelcastInstance instance = factory.getInstance(member.getAddress());
                    assertNotNull(getRaftNode(instance, METADATA_GROUP_ID));
                }
            }
        });

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                for (Member member : raftMembers.subList(metadataGroupSize, raftMembers.size())) {
                    HazelcastInstance instance = factory.getInstance(member.getAddress());
                    assertNull(getRaftNode(instance, METADATA_GROUP_ID));
                }
            }
        }, 10);
    }

    @Test
    public void when_raftGroupIsCreatedWithSomeCPNodes_then_raftNodeIsCreatedOnOnlyTheSelectedEndpoints() {
        when_raftGroupIsCreatedWithSomeCPNodes_then_raftNodeIsCreatedOnOnlyTheSelectedEndpoints(true);
    }

    @Test
    public void when_raftGroupIsCreatedFromNonCPNode_then_raftNodeIsCreatedOnOnlyTheSelectedEndpoints() {
        when_raftGroupIsCreatedWithSomeCPNodes_then_raftNodeIsCreatedOnOnlyTheSelectedEndpoints(false);
    }

    private void when_raftGroupIsCreatedWithSomeCPNodes_then_raftNodeIsCreatedOnOnlyTheSelectedEndpoints(boolean invokeOnCP) {
        int cpNodeCount = 5;
        int metadataGroupSize = 2;
        int nonCpNodeCount = 2;
        instances = newInstances(cpNodeCount, metadataGroupSize, nonCpNodeCount);

        final int newGroupCount = metadataGroupSize + 1;

        HazelcastInstance instance = instances[invokeOnCP ? 0 : instances.length - 1];
        final CPGroupId groupId = createNewRaftGroup(instance, "id", newGroupCount);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                int count = 0;
                for (HazelcastInstance instance : instances) {
                    RaftNodeImpl raftNode = getRaftNode(instance, groupId);
                    if (raftNode != null) {
                        count++;
                    }
                }

                assertEquals(newGroupCount, count);
            }
        });
    }

    @Test
    public void when_sizeOfRaftGroupIsLargerThanCPNodeCount_then_raftGroupCannotBeCreated() {
        int nodeCount = 3;
        instances = newInstances(nodeCount);

        try {
            createNewRaftGroup(instances[0], "id", nodeCount + 1);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    public void when_raftGroupIsCreatedWithSameSizeMultipleTimes_then_itSucceeds() {
        int nodeCount = 3;
        instances = newInstances(nodeCount);
        CPGroupId groupId1 = createNewRaftGroup(instances[0], "id", nodeCount);
        CPGroupId groupId2 = createNewRaftGroup(instances[1], "id", nodeCount);
        assertEquals(groupId1, groupId2);
    }

    @Test
    public void when_raftGroupIsCreatedWithDifferentSizeMultipleTimes_then_itFails() {
        int nodeCount = 3;
        instances = newInstances(nodeCount);

        createNewRaftGroup(instances[0], "id", nodeCount);
        try {
            createNewRaftGroup(instances[0], "id", nodeCount - 1);
            fail();
        } catch (IllegalStateException ignored) {
        }
    }

    @Test
    public void when_raftGroupDestroyTriggered_then_raftGroupIsDestroyed() {
        int metadataGroupSize = 3;
        int cpNodeCount = 5;
        instances = newInstances(cpNodeCount, metadataGroupSize, 0);

        final CPGroupId groupId = createNewRaftGroup(instances[0], "id", cpNodeCount);
        destroyRaftGroup(instances[0], groupId);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    assertNull(getRaftNode(instance, groupId));
                }
            }
        });

        final RaftInvocationManager invocationService = getRaftInvocationManager(instances[0]);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Future<RaftGroup> f = invocationService.query(METADATA_GROUP_ID, new GetRaftGroupOp(groupId),
                        LEADER_LOCAL);

                RaftGroup group = f.get();
                assertEquals(CPGroupStatus.DESTROYED, group.status());
            }
        });
    }

    @Test
    public void when_raftGroupDestroyTriggeredMultipleTimes_then_destroyDoesNotFail() {
        int metadataGroupSize = 3;
        int cpNodeCount = 5;
        instances = newInstances(cpNodeCount, metadataGroupSize, 0);

        CPGroupId groupId = createNewRaftGroup(instances[0], "id", cpNodeCount);
        destroyRaftGroup(instances[0], groupId);
        destroyRaftGroup(instances[0], groupId);
    }

    @Test
    public void when_raftGroupIsDestroyed_then_itCanBeCreatedAgain() {
        int metadataGroupSize = 3;
        int cpNodeCount = 5;
        instances = newInstances(cpNodeCount, metadataGroupSize, 0);

        final CPGroupId groupId = createNewRaftGroup(instances[0], "id", cpNodeCount);
        destroyRaftGroup(instances[0], groupId);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    assertNull(getRaftNode(instance, groupId));
                }
            }
        });

        createNewRaftGroup(instances[0], "id", cpNodeCount - 1);
    }

    @Test
    public void when_nonMetadataRaftGroupIsAlive_then_itCanBeForceDestroyed() throws ExecutionException, InterruptedException {
        instances = newInstances(3);

        waitAllForLeaderElection(instances, METADATA_GROUP_ID);

        final CPGroupId groupId = createNewRaftGroup(instances[0], "id", 3);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    assertNotNull(getRaftService(instance).getRaftNode(groupId));
                }
            }
        });

        getRaftService(instances[0]).forceDestroyCPGroup(groupId).get();

        RaftGroup groupInfo = getRaftInvocationManager(instances[0]).<RaftGroup>query(METADATA_GROUP_ID, new GetRaftGroupOp(groupId), LEADER_LOCAL).get();
        assertEquals(CPGroupStatus.DESTROYED, groupInfo.status());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    assertNull(getRaftNode(instance, groupId));
                }
            }
        });

        CPGroupId newGroupId = createNewRaftGroup(instances[0], "id", 3);
        assertNotEquals(groupId, newGroupId);
    }

    @Test
    public void when_nonMetadataRaftGroupLosesMajority_then_itCanBeForceDestroyed() throws ExecutionException, InterruptedException {
        instances = newInstances(5);

        waitAllForLeaderElection(instances, METADATA_GROUP_ID);

        final CPGroupId groupId = createNewRaftGroup(instances[0], "id", 3);

        RaftGroup groupInfo = getRaftInvocationManager(instances[0]).<RaftGroup>query(METADATA_GROUP_ID, new GetRaftGroupOp(groupId), LEADER_LOCAL).get();
        final CPMember[] groupMembers = groupInfo.membersArray();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (CPMember member : groupMembers) {
                    HazelcastInstance instance = factory.getInstance(member.getAddress());
                    assertNotNull(getRaftNode(instance, groupId));
                }
            }
        });

        factory.getInstance(groupMembers[0].getAddress()).getLifecycleService().terminate();
        factory.getInstance(groupMembers[1].getAddress()).getLifecycleService().terminate();

        final HazelcastInstance runningInstance = factory.getInstance(groupMembers[2].getAddress());
        getRaftService(runningInstance).forceDestroyCPGroup(groupId).get();

        groupInfo = getRaftInvocationManager(runningInstance).<RaftGroup>query(METADATA_GROUP_ID, new GetRaftGroupOp(groupId), LEADER_LOCAL).get();
        assertEquals(CPGroupStatus.DESTROYED, groupInfo.status());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNull(getRaftNode(runningInstance, groupId));
            }
        });
    }

    @Test
    public void when_metadataClusterNodeFallsFarBehind_then_itInstallsSnapshot() {
        int nodeCount = 3;
        int commitCountToSnapshot = 5;
        Config config = createConfig(nodeCount, nodeCount);
        config.getCpSubsystemConfig().getRaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(commitCountToSnapshot);

        final HazelcastInstance[] instances = new HazelcastInstance[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            instances[i] = factory.newHazelcastInstance(config);
        }

        waitAllForLeaderElection(instances, METADATA_GROUP_ID);
        CPMember leaderEndpoint = getLeaderMember(getRaftNode(instances[0], METADATA_GROUP_ID));

        final HazelcastInstance leader = factory.getInstance(leaderEndpoint.getAddress());
        HazelcastInstance follower = null;
        for (HazelcastInstance instance : instances) {
            if (!getAddress(instance).equals(leaderEndpoint.getAddress())) {
                follower = instance;
                break;
            }
        }

        assertNotNull(follower);
        blockCommunicationBetween(leader, follower);

        final List<CPGroupId> groupIds = new ArrayList<CPGroupId>();
        for (int i = 0; i < commitCountToSnapshot; i++) {
            CPGroupId groupId = createNewRaftGroup(leader, "id" + i, nodeCount);
            groupIds.add(groupId);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(getSnapshotEntry(getRaftNode(leader, METADATA_GROUP_ID)).index() > 0);
            }
        });

        unblockCommunicationBetween(leader, follower);

        final HazelcastInstance f = follower;
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (CPGroupId groupId : groupIds) {
                    assertNotNull(getRaftNode(f, groupId));
                }
            }
        });
    }

    @Test
    public void when_raftGroupIsCreated_onNonMetadataMembers_thenLeaderShouldBeElected() throws ExecutionException, InterruptedException {
        int metadataGroupSize = 3;
        int otherRaftGroupSize = 2;
        instances = newInstances(metadataGroupSize + otherRaftGroupSize, metadataGroupSize, 0);

        HazelcastInstance leaderInstance = getLeaderInstance(instances, METADATA_GROUP_ID);
        final RaftService raftService = getRaftService(leaderInstance);
        Collection<CPMember> allEndpoints = raftService.getMetadataGroupManager().getActiveMembers();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNotNull(raftService.getCPGroup(METADATA_GROUP_ID));
            }
        });
        RaftGroup metadataGroup = raftService.getCPGroup(METADATA_GROUP_ID);

        final Collection<CPMember> endpoints = new HashSet<CPMember>(otherRaftGroupSize);
        for (CPMember endpoint : allEndpoints) {
            if (!metadataGroup.containsMember(endpoint)) {
                endpoints.add(endpoint);
            }
        }
        assertEquals(otherRaftGroupSize, endpoints.size());

        ICompletableFuture<CPGroupId> f = raftService.getInvocationManager()
                                                     .invoke(METADATA_GROUP_ID, new CreateRaftGroupOp("test", endpoints));

        final CPGroupId groupId = f.get();

        for (final HazelcastInstance instance : instances) {
            if (endpoints.contains(getRaftService(instance).getLocalMember())) {
                assertTrueEventually(new AssertTask() {
                    @Override
                    public void run() {
                        RaftNodeImpl raftNode = getRaftNode(instance, groupId);
                        assertNotNull(raftNode);
                        waitUntilLeaderElected(raftNode);
                    }
                });
            }
        }
    }

    @Test
    public void when_shutdownLeader_thenNewLeaderElected() {
        int cpNodeCount = 5;
        int metadataGroupSize = cpNodeCount - 1;
        instances = newInstances(cpNodeCount, metadataGroupSize, 0);

        HazelcastInstance leaderInstance = getLeaderInstance(instances, METADATA_GROUP_ID);
        leaderInstance.shutdown();

        for (HazelcastInstance instance : instances) {
            if (instance == leaderInstance) {
                continue;
            }
            RaftNodeImpl raftNode = getRaftNode(instance, METADATA_GROUP_ID);
            while (raftNode == null) {
                sleepMillis(100);
                raftNode = getRaftNode(instance, METADATA_GROUP_ID);
            }
            waitUntilLeaderElected(raftNode);
        }
    }

    @Test
    public void when_memberIsShutdown_then_itIsRemovedFromRaftGroups() throws ExecutionException, InterruptedException {
        int cpNodeCount = 5;
        int metadataGroupSize = cpNodeCount - 1;
        int atomicLong1GroupSize = cpNodeCount - 2;
        instances = newInstances(cpNodeCount, metadataGroupSize, 0);

        CPGroupId groupId1 = createNewRaftGroup(instances[0], "id1", atomicLong1GroupSize);
        CPGroupId groupId2 = createNewRaftGroup(instances[0], "id2", cpNodeCount);

        CPMember endpoint = findCommonEndpoint(instances[0], METADATA_GROUP_ID, groupId1);
        assertNotNull(endpoint);

        RaftInvocationManager invocationService = null;
        for (HazelcastInstance instance : instances) {
            if (!getAddress(instance).equals(endpoint.getAddress())) {
                invocationService = getRaftInvocationManager(instance);
                break;
            }
        }
        assertNotNull(invocationService);

        factory.getInstance(endpoint.getAddress()).shutdown();

        ICompletableFuture<List<CPMember>> f1 = invocationService.query(METADATA_GROUP_ID, new GetActiveCPMembersOp(),
                LEADER_LOCAL);

        List<CPMember> activeEndpoints = f1.get();
        assertThat(activeEndpoints, not(hasItem(endpoint)));

        ICompletableFuture<RaftGroup> f2 = invocationService.query(METADATA_GROUP_ID, new GetRaftGroupOp(METADATA_GROUP_ID),
                LEADER_LOCAL);

        ICompletableFuture<RaftGroup> f3 = invocationService.query(METADATA_GROUP_ID, new GetRaftGroupOp(groupId1),
                LEADER_LOCAL);

        ICompletableFuture<RaftGroup> f4 = invocationService.query(METADATA_GROUP_ID, new GetRaftGroupOp(groupId2),
                LEADER_LOCAL);

        RaftGroup metadataGroup = f2.get();
        assertFalse(metadataGroup.containsMember(endpoint));
        assertEquals(metadataGroupSize, metadataGroup.memberCount());
        RaftGroup atomicLongGroup1 = f3.get();
        assertFalse(atomicLongGroup1.containsMember(endpoint));
        assertEquals(atomicLong1GroupSize, atomicLongGroup1.memberCount());
        RaftGroup atomicLongGroup2 = f4.get();
        assertFalse(atomicLongGroup2.containsMember(endpoint));
        assertEquals(cpNodeCount - 1, atomicLongGroup2.memberCount());
    }

    @Test
    public void when_nonReachableEndpointsExist_createRaftGroupPrefersReachableEndpoints()
            throws ExecutionException, InterruptedException {
        final HazelcastInstance[] instances = newInstances(5);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    Collection<CPMember> raftMembers = getRaftService(instance).getMetadataGroupManager().getActiveMembers();
                    assertFalse(raftMembers.isEmpty());
                }
            }
        });

        CPMember endpoint3 = getRaftService(instances[3]).getLocalMember();
        CPMember endpoint4 = getRaftService(instances[4]).getLocalMember();
        instances[3].getLifecycleService().terminate();
        instances[4].getLifecycleService().terminate();

        RaftInvocationManager invocationManager = getRaftInvocationManager(instances[0]);
        CPGroupId g3 = invocationManager.createRaftGroup("g3", 3).get();
        CPGroupId g4 = invocationManager.createRaftGroup("g4", 4).get();

        RaftNodeImpl leaderNode = waitAllForLeaderElection(Arrays.copyOf(instances, 3), METADATA_GROUP_ID);
        HazelcastInstance leader = factory.getInstance(((CPMember) leaderNode.getLocalMember()).getAddress());
        RaftGroup g3Group = getRaftGroupLocally(leader, g3);
        assertThat(g3Group.memberImpls(), not(hasItem(endpoint3)));
        assertThat(g3Group.memberImpls(), not(hasItem(endpoint4)));

        RaftGroup g4Group = getRaftGroupLocally(leader, g4);
        boolean b3 = g4Group.containsMember(endpoint3);
        boolean b4 = g4Group.containsMember(endpoint4);
        assertTrue(b3 ^ b4);
    }

    @Test
    public void when_noCpNodeCountConfigured_then_cpDiscoveryCompletes() {
        final HazelcastInstance[] instances = newInstances(0, 0, 3);

        waitUntilCPDiscoveryCompleted(instances);
    }

    private CPGroupId createNewRaftGroup(HazelcastInstance instance, String name, int groupSize) {
        RaftInvocationManager invocationManager = getRaftService(instance).getInvocationManager();
        try {
            return invocationManager.createRaftGroup(name, groupSize).get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private void destroyRaftGroup(HazelcastInstance instance, CPGroupId groupId) {
        RaftInvocationManager invocationManager = getRaftService(instance).getInvocationManager();
        try {
            invocationManager.triggerDestroy(groupId).get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private CPMember findCommonEndpoint(HazelcastInstance instance, final CPGroupId groupId1, final CPGroupId groupId2)
            throws ExecutionException, InterruptedException {
        RaftInvocationManager invocationService = getRaftInvocationManager(instance);
        ICompletableFuture<RaftGroup> f1 = invocationService.query(METADATA_GROUP_ID, new GetRaftGroupOp(groupId1),
                LEADER_LOCAL);
        ICompletableFuture<RaftGroup> f2 = invocationService.query(METADATA_GROUP_ID, new GetRaftGroupOp(groupId2),
                LEADER_LOCAL);
        RaftGroup group1 = f1.get();
        RaftGroup group2 = f2.get();

        Set<CPMember> members = new HashSet<CPMember>(group1.memberImpls());
        members.retainAll(group2.members());

        return members.isEmpty() ? null : members.iterator().next();
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);
        config.getCpSubsystemConfig().getRaftAlgorithmConfig().setLeaderHeartbeatPeriodInMillis(1000);
        return config;
    }

}
