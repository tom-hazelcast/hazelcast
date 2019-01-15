package com.hazelcast.raft.impl.handler;

import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.dto.PreVoteRequest;
import com.hazelcast.raft.impl.dto.PreVoteResponse;
import com.hazelcast.raft.impl.log.RaftLog;
import com.hazelcast.raft.impl.state.RaftState;
import com.hazelcast.raft.impl.task.RaftNodeAwareTask;
import com.hazelcast.util.Clock;

/**
 * TODO: Javadoc Pending...
 *
 */
public class PreVoteRequestHandlerTask extends RaftNodeAwareTask implements Runnable {
    private final PreVoteRequest req;

    public PreVoteRequestHandlerTask(RaftNode raftNode, PreVoteRequest req) {
        super(raftNode);
        this.req = req;
    }

    @Override
    protected void innerRun() {
        RaftState state = raftNode.state();
        RaftEndpoint localEndpoint = raftNode.getLocalEndpoint();

        // Reply false if term < currentTerm (§5.1)
        if (state.term() > req.nextTerm()) {
            logger.info("Rejecting " + req + " since current term: " + state.term() + " is bigger");
            raftNode.send(new PreVoteResponse(localEndpoint, state.term(), false), req.candidate());
            return;
        }

        // Reply false if last AppendEntries call was received less than election timeout ago (leader stickiness)
        if (raftNode.lastAppendEntriesTimestamp() > Clock.currentTimeMillis() - raftNode.getLeaderElectionTimeoutInMillis()) {
            logger.info("Rejecting " + req + " since received append entries recently.");
            raftNode.send(new PreVoteResponse(localEndpoint, state.term(), false), req.candidate());
            return;
        }

        RaftLog raftLog = state.log();
        if (raftLog.lastLogOrSnapshotTerm() > req.lastLogTerm()) {
            logger.info("Rejecting " + req + " since our last log term: " + raftLog.lastLogOrSnapshotTerm() + " is greater");
            raftNode.send(new PreVoteResponse(localEndpoint, req.nextTerm(), false), req.candidate());
            return;
        }

        if (raftLog.lastLogOrSnapshotTerm() == req.lastLogTerm() && raftLog.lastLogOrSnapshotIndex() > req.lastLogIndex()) {
            logger.info("Rejecting " + req + " since our last log index: " + raftLog.lastLogOrSnapshotIndex() + " is greater");
            raftNode.send(new PreVoteResponse(localEndpoint, req.nextTerm(), false), req.candidate());
            return;
        }

        logger.info("Granted pre-vote for " + req);
        raftNode.send(new PreVoteResponse(localEndpoint, req.nextTerm(), true), req.candidate());
    }
}
