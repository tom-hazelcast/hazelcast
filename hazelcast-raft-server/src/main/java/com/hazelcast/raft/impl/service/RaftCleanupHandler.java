package com.hazelcast.raft.impl.service;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.MembershipChangeType;
import com.hazelcast.raft.QueryPolicy;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.exception.MismatchingGroupMembersCommitIndexException;
import com.hazelcast.raft.exception.RaftGroupTerminatedException;
import com.hazelcast.raft.impl.RaftMemberImpl;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftNodeStatus;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.service.MembershipChangeContext.RaftGroupMembershipChangeContext;
import com.hazelcast.raft.impl.service.RaftGroupInfo.RaftGroupStatus;
import com.hazelcast.raft.impl.service.operation.metadata.CompleteDestroyRaftGroupsOp;
import com.hazelcast.raft.impl.service.operation.metadata.CompleteRaftGroupMembershipChangesOp;
import com.hazelcast.raft.impl.service.operation.metadata.DestroyRaftNodesOp;
import com.hazelcast.raft.impl.service.operation.metadata.GetDestroyingRaftGroupIdsOp;
import com.hazelcast.raft.impl.service.operation.metadata.GetMembershipChangeContextOp;
import com.hazelcast.raft.impl.service.operation.metadata.GetRaftGroupOp;
import com.hazelcast.raft.impl.service.operation.metadata.TriggerExpandRaftGroupsOp;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.RandomPicker;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

import static com.hazelcast.raft.impl.service.RaftService.METADATA_GROUP_ID;
import static com.hazelcast.spi.ExecutionService.ASYNC_EXECUTOR;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftCleanupHandler {

    static final long CLEANUP_TASK_PERIOD_IN_MILLIS = SECONDS.toMillis(1);
    private static final long CHECK_LOCAL_RAFT_NODES_TASK_PERIOD_IN_MILLIS = SECONDS.toMillis(10);

    private final NodeEngine nodeEngine;
    private final RaftService raftService;
    private final ILogger logger;
    private volatile RaftInvocationManager invocationManager;

    RaftCleanupHandler(NodeEngine nodeEngine, RaftService raftService) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.raftService = raftService;
    }

    void init() {
        if (getLocalMember() == null) {
            return;
        }

        this.invocationManager = raftService.getInvocationManager();

        ExecutionService executionService = nodeEngine.getExecutionService();
        // scheduleWithRepetition skips subsequent execution if one is already running.
        executionService.scheduleWithRepetition(new RaftGroupDestroyHandlerTask(), CLEANUP_TASK_PERIOD_IN_MILLIS,
                CLEANUP_TASK_PERIOD_IN_MILLIS, MILLISECONDS);
        executionService.scheduleWithRepetition(new RemoveRaftMemberHandlerTask(), CLEANUP_TASK_PERIOD_IN_MILLIS,
                CLEANUP_TASK_PERIOD_IN_MILLIS, MILLISECONDS);
        executionService.scheduleWithRepetition(new CheckLocalRaftNodesTask(), CHECK_LOCAL_RAFT_NODES_TASK_PERIOD_IN_MILLIS,
                CHECK_LOCAL_RAFT_NODES_TASK_PERIOD_IN_MILLIS, MILLISECONDS);
    }

    private RaftMemberImpl getLocalMember() {
        return raftService.getMetadataManager().getLocalMember();
    }

    private boolean skipRunningCleanupTask() {
        return !raftService.getMetadataManager().isMetadataLeader();
    }

    private class CheckLocalRaftNodesTask implements Runnable {

        public void run() {
            for (RaftNode raftNode : raftService.getAllRaftNodes()) {
                final RaftGroupId groupId = raftNode.getGroupId();
                if (groupId.equals(METADATA_GROUP_ID)) {
                    continue;
                }

                if (raftNode.getStatus() == RaftNodeStatus.TERMINATED || raftNode.getStatus() == RaftNodeStatus.STEPPED_DOWN) {
                    raftService.destroyRaftNode(groupId);
                    continue;
                }

                ICompletableFuture<RaftGroupInfo> f = queryMetadata(new GetRaftGroupOp(groupId));

                f.andThen(new ExecutionCallback<RaftGroupInfo>() {
                    @Override
                    public void onResponse(RaftGroupInfo group) {
                        if (group == null) {
                            logger.severe("Could not find raft group for local raft node of " + groupId);
                        } else if (group.status() == RaftGroupStatus.DESTROYED) {
                            raftService.destroyRaftNode(groupId);
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        logger.warning("Could not get raft group info of " + groupId, t);
                    }
                });
            }
        }

    }

    private class RaftGroupDestroyHandlerTask implements Runnable {
        @Override
        public void run() {
            if (skipRunningCleanupTask()) {
                return;
            }

            Collection<RaftGroupId> destroyingRaftGroupIds = getDestroyingRaftGroupIds();
            if (destroyingRaftGroupIds.isEmpty()) {
                return;
            }

            Map<RaftGroupId, Future<Object>> futures = new HashMap<RaftGroupId, Future<Object>>();
            for (RaftGroupId groupId : destroyingRaftGroupIds) {
                Future<Object> future = invocationManager.terminate(groupId);
                futures.put(groupId, future);
            }

            Set<RaftGroupId> terminatedGroupIds = new HashSet<RaftGroupId>();
            for (Entry<RaftGroupId, Future<Object>> e : futures.entrySet()) {
                if (isRaftGroupTerminated(e.getKey(), e.getValue())) {
                    terminatedGroupIds.add(e.getKey());
                }
            }

            if (terminatedGroupIds.isEmpty()) {
                return;
            }

            commitDestroyedRaftGroups(terminatedGroupIds);

            for (RaftGroupId groupId : terminatedGroupIds) {
                raftService.destroyRaftNode(groupId);
            }

            OperationService operationService = nodeEngine.getOperationService();
            for (RaftMemberImpl member : raftService.getMetadataManager().getActiveMembers()) {
                if (!member.equals(raftService.getLocalMember())) {
                    operationService.send(new DestroyRaftNodesOp(terminatedGroupIds), member.getAddress());
                }
            }
        }

        private Collection<RaftGroupId> getDestroyingRaftGroupIds() {
            Future<Collection<RaftGroupId>> f = queryMetadata(new GetDestroyingRaftGroupIdsOp());

            try {
                return f.get();
            } catch (Exception e) {
                logger.severe("Cannot get destroying raft group ids", e);
                return Collections.emptyList();
            }
        }

        private boolean isRaftGroupTerminated(RaftGroupId groupId, Future<Object> future) {
            try {
                future.get();
                return true;
            }  catch (InterruptedException e) {
                logger.severe("Cannot get result of DESTROY commit to " + groupId, e);
                return false;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof RaftGroupTerminatedException) {
                    return true;
                }

                logger.severe("Cannot get result of DESTROY commit to " + groupId, e);

                return false;
            }
        }

        private void commitDestroyedRaftGroups(Set<RaftGroupId> destroyedGroupIds) {
            RaftOp op = new CompleteDestroyRaftGroupsOp(destroyedGroupIds);
            Future<Collection<RaftGroupId>> f = invocationManager.invoke(METADATA_GROUP_ID, op);

            try {
                f.get();
                logger.info("Terminated raft groups: " + destroyedGroupIds + " are committed.");
            } catch (Exception e) {
                logger.severe("Cannot commit terminated raft groups: " + destroyedGroupIds, e);
            }
        }
    }

    private class RemoveRaftMemberHandlerTask implements Runnable {

        private static final int NA_MEMBERS_COMMIT_INDEX = -1;

        @Override
        public void run() {
            if (skipRunningCleanupTask()) {
                return;
            }

            MembershipChangeContext membershipChangeContext = decideMembersToAddIfNeeded(getMembershipChangeContext());
            if (membershipChangeContext == null) {
                return;
            }

            handleMembershipChanges(membershipChangeContext);
        }

        private MembershipChangeContext getMembershipChangeContext() {
            Future<MembershipChangeContext> f = queryMetadata(new GetMembershipChangeContextOp());

            try {
                return f.get();
            } catch (Exception e) {
                logger.severe("Cannot get membership change context", e);
                return null;
            }
        }

        private MembershipChangeContext decideMembersToAddIfNeeded(MembershipChangeContext membershipChangeContext) {
            if (membershipChangeContext == null) {
                return null;
            } else if (membershipChangeContext.hasNoPendingMembersToAdd()) {
                return membershipChangeContext;
            }

            Map<RaftGroupId, RaftMemberImpl> membersToAdd = new HashMap<RaftGroupId, RaftMemberImpl>();
            for (Entry<RaftGroupId, List<RaftMemberImpl>> e : membershipChangeContext.getMemberMissingGroups().entrySet()) {
                List<RaftMemberImpl> candidates = e.getValue();
                int idx = RandomPicker.getInt(candidates.size());
                RaftMemberImpl memberToAdd = candidates.get(idx);
                membersToAdd.put(e.getKey(), memberToAdd);
            }

            RaftOp op = new TriggerExpandRaftGroupsOp(membersToAdd);
            ICompletableFuture<MembershipChangeContext> future = invocationManager.invoke(METADATA_GROUP_ID, op);

            try {
                return future.get();
            } catch (Exception e) {
                logger.severe("Cannot commit members to add: " + membersToAdd, e);
                return null;
            }
        }

        private void handleMembershipChanges(MembershipChangeContext membershipChangeContext) {
            logger.fine("Handling " + membershipChangeContext);

            Map<RaftGroupId, Future<Long>> memberAddFutures = new HashMap<RaftGroupId, Future<Long>>();
            Map<RaftGroupId, RaftGroupMembershipChangeContext> changingGroups = membershipChangeContext.getChanges();
            for (Entry<RaftGroupId, RaftGroupMembershipChangeContext> e : changingGroups.entrySet()) {
                RaftGroupId groupId = e.getKey();
                RaftGroupMembershipChangeContext groupCtx = e.getValue();

                if (groupCtx.getMemberToAdd() == null) {
                    memberAddFutures.put(groupId, newCompletedFuture(groupCtx.getMembersCommitIndex()));
                    continue;
                }

                logger.fine("Adding " + groupCtx.getMemberToAdd() + " to " + groupId);

                ICompletableFuture<Long> f = invocationManager.changeRaftGroupMembership(groupId,
                        groupCtx.getMembersCommitIndex(), groupCtx.getMemberToAdd(), MembershipChangeType.ADD);
                memberAddFutures.put(groupId, f);
            }

            Map<RaftGroupId, Future<Long>> memberRemoveFutures = new HashMap<RaftGroupId, Future<Long>>();
            for (Entry<RaftGroupId, Future<Long>> entry : memberAddFutures.entrySet()) {
                RaftGroupId groupId = entry.getKey();
                RaftGroupMembershipChangeContext groupCtx = changingGroups.get(groupId);
                long idx = getMemberAddCommitIndex(groupId, groupCtx, entry.getValue());
                if (idx != NA_MEMBERS_COMMIT_INDEX) {
                    if (groupCtx.getMemberToRemove() == null) {
                        memberRemoveFutures.put(groupId, newCompletedFuture(idx));
                        continue;
                    }

                    ICompletableFuture<Long> f = invocationManager.changeRaftGroupMembership(groupId, idx,
                            groupCtx.getMemberToRemove(), MembershipChangeType.REMOVE);
                    memberRemoveFutures.put(groupId, f);
                }
            }

            Map<RaftGroupId, Tuple2<Long, Long>> changedGroups = new HashMap<RaftGroupId, Tuple2<Long, Long>>();
            for (Entry<RaftGroupId, Future<Long>> entry : memberRemoveFutures.entrySet()) {
                RaftGroupId groupId = entry.getKey();
                RaftGroupMembershipChangeContext groupCtx = changingGroups.get(groupId);
                RaftMemberImpl memberToRemove = groupCtx.getMemberToRemove();
                long idx = getMemberRemoveCommitIndex(groupId, memberToRemove, groupCtx, entry.getValue());
                if (idx != NA_MEMBERS_COMMIT_INDEX) {
                    if (memberToRemove != null) {
                        logger.info(memberToRemove + " is removed from " + groupId + " with new members commit index: " + idx);
                    }
                    changedGroups.put(groupId, Tuple2.of(groupCtx.getMembersCommitIndex(), idx));
                }
            }

            completeMembershipChanges(changedGroups);
        }

        private SimpleCompletableFuture<Long> newCompletedFuture(long idx) {
            Executor executor = nodeEngine.getExecutionService().getExecutor(ASYNC_EXECUTOR);
            ILogger logger = nodeEngine.getLogger(getClass());
            SimpleCompletableFuture<Long> f = new SimpleCompletableFuture<Long>(executor, logger);
            f.setResult(idx);
            return f;
        }

        private long getMemberAddCommitIndex(RaftGroupId groupId, RaftGroupMembershipChangeContext ctx, Future<Long> future) {
            try {
                return future.get();
            }  catch (InterruptedException e) {
                logger.severe("Cannot get MEMBER ADD result of " + ctx.getMemberToAdd() + " to " + groupId
                        + " with members commit index: " + ctx.getMembersCommitIndex(), e);
                return NA_MEMBERS_COMMIT_INDEX;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof MismatchingGroupMembersCommitIndexException) {
                    MismatchingGroupMembersCommitIndexException m = (MismatchingGroupMembersCommitIndexException) e.getCause();

                    String msg = "MEMBER ADD commit of " + ctx.getMemberToAdd() + " to " + groupId
                            + " with members commit index: " + ctx.getMembersCommitIndex() + " failed. Actual group members: "
                            + m.getMembers() + " with commit index: " + m.getCommitIndex();

                    if (m.getMembers().size() != ctx.getMembers().size() + 1) {
                        logger.severe(msg);
                        return NA_MEMBERS_COMMIT_INDEX;
                    }

                    // learnt group members must contain the added member and the current members I know

                    if (!m.getMembers().contains(ctx.getMemberToAdd())) {
                        logger.severe(msg);
                        return NA_MEMBERS_COMMIT_INDEX;
                    }

                    for (RaftMemberImpl member : ctx.getMembers()) {
                        if (!m.getMembers().contains(member)) {
                            logger.severe(msg);
                            return NA_MEMBERS_COMMIT_INDEX;
                        }
                    }

                    return m.getCommitIndex();
                }

                logger.severe("Cannot get MEMBER ADD result of " + ctx.getMemberToAdd() + " to " + groupId
                        + " with members commit index: " + ctx.getMembersCommitIndex(), e);
                return NA_MEMBERS_COMMIT_INDEX;
            }
        }

        private long getMemberRemoveCommitIndex(RaftGroupId groupId, RaftMemberImpl removedMember,
                                                RaftGroupMembershipChangeContext ctx, Future<Long> future) {
            try {
                return future.get();
            }  catch (InterruptedException e) {
                logger.severe("Cannot get MEMBER REMOVE result of " + removedMember + " to " + groupId, e);
                return NA_MEMBERS_COMMIT_INDEX;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof MismatchingGroupMembersCommitIndexException) {
                    MismatchingGroupMembersCommitIndexException m = (MismatchingGroupMembersCommitIndexException) e.getCause();

                    String msg = "MEMBER REMOVE commit of " + removedMember + " to " + groupId
                            + " failed. Actual group members: " + m.getMembers() + " with commit index: " + m.getCommitIndex();

                    if (m.getMembers().contains(removedMember)) {
                        logger.severe(msg);
                        return NA_MEMBERS_COMMIT_INDEX;
                    }

                    if (ctx.getMemberToAdd() != null) {
                        // I expect the added member to be joined to the group
                        if (!m.getMembers().contains(ctx.getMemberToAdd())) {
                            logger.severe(msg);
                            return NA_MEMBERS_COMMIT_INDEX;
                        }

                        // I know the removed member has left the group and the added member has joined.
                        // So member sizes must be same...
                        if (m.getMembers().size() != ctx.getMembers().size()) {
                            logger.severe(msg);
                            return NA_MEMBERS_COMMIT_INDEX;
                        }
                    } else if (m.getMembers().size() != (ctx.getMembers().size() - 1)) {
                        // if there is no added member, I expect number of the learnt group members to be 1 less than
                        // the current members I know
                        logger.severe(msg);
                        return NA_MEMBERS_COMMIT_INDEX;
                    }

                    for (RaftMemberImpl member : ctx.getMembers()) {
                        // Other group members except the removed one and added one must be still present...
                        if (!member.equals(removedMember) && !m.getMembers().contains(member)) {
                            logger.severe(msg);
                            return NA_MEMBERS_COMMIT_INDEX;
                        }
                    }

                    return m.getCommitIndex();
                }

                logger.severe("Cannot get MEMBER REMOVE result of " + removedMember + " to " + groupId, e);
                return NA_MEMBERS_COMMIT_INDEX;
            }
        }

        private void completeMembershipChanges(Map<RaftGroupId, Tuple2<Long, Long>> changedGroups) {
            RaftOp op = new CompleteRaftGroupMembershipChangesOp(changedGroups);
            ICompletableFuture<Object> future = invocationManager.invoke(METADATA_GROUP_ID, op);

            try {
                future.get();
            } catch (Exception e) {
                logger.severe("Cannot commit raft group membership changes: " + changedGroups, e);
            }
        }
    }

    private <T> ICompletableFuture<T> queryMetadata(RaftOp raftOp) {
        return invocationManager.query(METADATA_GROUP_ID, raftOp, QueryPolicy.LEADER_LOCAL);
    }
}
