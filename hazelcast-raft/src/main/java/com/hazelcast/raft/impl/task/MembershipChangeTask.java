package com.hazelcast.raft.impl.task;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.MembershipChangeType;
import com.hazelcast.raft.exception.MemberAlreadyExistsException;
import com.hazelcast.raft.exception.MemberDoesNotExistException;
import com.hazelcast.raft.exception.MismatchingGroupMembersCommitIndexException;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.operation.ApplyRaftGroupMembersOp;
import com.hazelcast.raft.impl.state.RaftGroupMembers;
import com.hazelcast.raft.impl.state.RaftState;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;

import java.util.Collection;
import java.util.LinkedHashSet;

/**
 * TODO: Javadoc Pending...
 *
 */
public class MembershipChangeTask implements Runnable {
    private final RaftNodeImpl raftNode;
    private final Integer groupMembersCommitIndex;
    private final RaftEndpoint member;
    private final MembershipChangeType changeType;
    private final SimpleCompletableFuture resultFuture;
    private final ILogger logger;

    public MembershipChangeTask(RaftNodeImpl raftNode, SimpleCompletableFuture resultFuture, RaftEndpoint member,
                                MembershipChangeType changeType) {
        this(raftNode, resultFuture, member, changeType, null);
    }

    public MembershipChangeTask(RaftNodeImpl raftNode, SimpleCompletableFuture resultFuture, RaftEndpoint member,
                                MembershipChangeType changeType, Integer groupMembersCommitIndex) {
        if (changeType == null) {
            throw new IllegalArgumentException("Null membership change type");
        }
        this.raftNode = raftNode;
        this.groupMembersCommitIndex = groupMembersCommitIndex;
        this.member = member;
        this.changeType = changeType;
        this.resultFuture = resultFuture;
        this.logger = raftNode.getLogger(getClass());
    }

    @Override
    public void run() {
        if (!isValidGroupMemberCommitIndex()) {
            return;
        }

        logger.severe("Changing membership -> " + changeType + ": " + member);

        RaftState state = raftNode.state();
        Collection<RaftEndpoint> members = new LinkedHashSet<RaftEndpoint>(state.members());
        boolean memberExists = members.contains(member);

        switch (changeType) {
            case ADD:
                if (memberExists) {
                    resultFuture.setResult(new MemberAlreadyExistsException(member));
                    return;
                }
                members.add(member);
                break;

            case REMOVE:
                if (!memberExists) {
                    resultFuture.setResult(new MemberDoesNotExistException(member));
                    return;
                }
                members.remove(member);
                break;

            default:
                resultFuture.setResult(new IllegalArgumentException("Unknown type: " + changeType));
                return;
        }
        logger.severe("New members after " + changeType + " -> " + members);
        new ReplicateTask(raftNode, new ApplyRaftGroupMembersOp(members), resultFuture).run();
    }

    private boolean isValidGroupMemberCommitIndex() {
        if (groupMembersCommitIndex != null) {
            RaftState state = raftNode.state();
            RaftGroupMembers groupMembers = state.committedGroupMembers();
            if (groupMembers.index() != groupMembersCommitIndex) {
                logger.severe("Cannot " + changeType + " " + member + " because expected members commit index: "
                        + groupMembersCommitIndex + " is different than group members commit index: " + groupMembers.index());

                Exception e = new MismatchingGroupMembersCommitIndexException(groupMembers.index(), groupMembers.members());
                resultFuture.setResult(e);
                return false;
            }
        }
        return true;
    }
}
