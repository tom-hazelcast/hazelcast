package com.hazelcast.raft.impl.task;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.QueryPolicy;
import com.hazelcast.raft.exception.NotLeaderException;
import com.hazelcast.raft.exception.RaftGroupTerminatedException;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftNode.RaftNodeStatus;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.raft.impl.state.RaftState;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;
import com.hazelcast.raft.operation.RaftCommandOperation;
import com.hazelcast.raft.operation.RaftOperation;

/**
 * TODO: Javadoc Pending...
 *
 */
public class QueryTask implements Runnable {
    private final RaftNode raftNode;
    private final RaftOperation operation;
    private final QueryPolicy queryPolicy;
    private final SimpleCompletableFuture resultFuture;
    private final ILogger logger;

    public QueryTask(RaftNode raftNode, RaftOperation operation, QueryPolicy policy, SimpleCompletableFuture resultFuture) {
        this.raftNode = raftNode;
        this.operation = operation;
        this.logger = raftNode.getLogger(getClass());
        this.queryPolicy = policy;
        this.resultFuture = resultFuture;
    }

    @Override
    public void run() {
        if (!verifyOperation()) {
            return;
        }

        if (!verifyRaftNodeStatus()) {
            return;
        }

        switch (queryPolicy) {
            case LEADER_LOCAL:
                handleLeaderLocalRead();
                break;
            case ANY_LOCAL:
                handleAnyLocalRead();
                break;
            default:
                resultFuture.setResult(new IllegalArgumentException("Invalid query policy: " + queryPolicy));
        }
    }

    private void handleLeaderLocalRead() {
        RaftState state = raftNode.state();
        if (state.role() != RaftRole.LEADER) {
            resultFuture.setResult(new NotLeaderException(raftNode.getGroupId(), raftNode.getLocalEndpoint(), state.leader()));
            return;
        }

        // TODO: We can reject the query, if leader is not able to reach majority of the followers

        handleAnyLocalRead();
    }

    private void handleAnyLocalRead() {
        RaftState state = raftNode.state();
        if (logger.isFineEnabled()) {
            logger.fine("Querying: " + operation + " with policy: " + queryPolicy + " in term: " + state.term());
        }

        // TODO: We can reject the query, if follower have not received any heartbeat recently

        Object result = raftNode.runQueryOperation(operation);
        resultFuture.setResult(result);
    }

    private boolean verifyOperation() {
        if (!(raftNode.getServiceName().equals(operation.getServiceName()))) {
            resultFuture.setResult(new IllegalArgumentException("operation: " + operation + "  service name: "
                    + operation.getServiceName() + " is different than expected service name: " + raftNode.getServiceName()));
            return false;
        }

        if (operation instanceof RaftCommandOperation) {
            resultFuture.setResult(new IllegalArgumentException(operation + " cannot query " + raftNode.getServiceName()));
            return false;
        }

        return true;
    }

    private boolean verifyRaftNodeStatus() {
        if (raftNode.getStatus() == RaftNodeStatus.TERMINATED) {
            resultFuture.setResult(new RaftGroupTerminatedException());
            return false;
        } else if (raftNode.getStatus() == RaftNodeStatus.STEPPED_DOWN) {
            resultFuture.setResult(new NotLeaderException(raftNode.getGroupId(), raftNode.getLocalEndpoint(), null));
            return false;
        }

        return true;
    }

}
