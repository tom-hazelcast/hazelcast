package com.hazelcast.raft;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.util.Preconditions.checkPositive;
import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftConfig {


    public static final long DEFAULT_LEADER_ELECTION_TIMEOUT_IN_MILLIS = 2000;

    public static final long DEFAULT_LEADER_HEARTBEAT_PERIOD_IN_MILLIS = 5000;

    public static final int DEFAULT_APPEND_REQUEST_MAX_ENTRY_COUNT = 50;

    public static final int DEFAULT_COMMIT_INDEX_ADVANCE_COUNT_TO_SNAPSHOT = 1000;

    public static final int DEFAULT_UNCOMMITTED_ENTRY_COUNT_TO_REJECT_NEW_APPENDS = 100;


    private long leaderElectionTimeoutInMillis = DEFAULT_LEADER_ELECTION_TIMEOUT_IN_MILLIS;

    private long leaderHeartbeatPeriodInMillis = DEFAULT_LEADER_HEARTBEAT_PERIOD_IN_MILLIS;

    private int appendRequestMaxEntryCount = DEFAULT_APPEND_REQUEST_MAX_ENTRY_COUNT;

    private int commitIndexAdvanceCountToSnapshot = DEFAULT_COMMIT_INDEX_ADVANCE_COUNT_TO_SNAPSHOT;

    private int uncommittedEntryCountToRejectNewAppends = DEFAULT_UNCOMMITTED_ENTRY_COUNT_TO_REJECT_NEW_APPENDS;

    private boolean failOnIndeterminateOperationState;

    // See https://groups.google.com/forum/#!msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J
    private boolean appendNopEntryOnLeaderElection;

    private final List<RaftMember> members = new ArrayList<RaftMember>();

    private int metadataGroupSize;

    public RaftConfig() {
    }

    public RaftConfig(RaftConfig config) {
        this.leaderElectionTimeoutInMillis = config.leaderElectionTimeoutInMillis;
        this.leaderHeartbeatPeriodInMillis = config.leaderHeartbeatPeriodInMillis;
        this.appendRequestMaxEntryCount = config.appendRequestMaxEntryCount;
        this.commitIndexAdvanceCountToSnapshot = config.commitIndexAdvanceCountToSnapshot;
        this.uncommittedEntryCountToRejectNewAppends = config.uncommittedEntryCountToRejectNewAppends;
        this.failOnIndeterminateOperationState = config.failOnIndeterminateOperationState;
        this.appendNopEntryOnLeaderElection = config.appendNopEntryOnLeaderElection;
        for (RaftMember member : config.members) {
            this.members.add(new RaftMember(member));
        }
        this.metadataGroupSize = config.metadataGroupSize;
    }

    public long getLeaderElectionTimeoutInMillis() {
        return leaderElectionTimeoutInMillis;
    }

    public RaftConfig setLeaderElectionTimeoutInMillis(long leaderElectionTimeoutInMillis) {
        checkPositive(leaderElectionTimeoutInMillis, "leader election timeout in millis: "
                + leaderElectionTimeoutInMillis + " should be positive");
        this.leaderElectionTimeoutInMillis = leaderElectionTimeoutInMillis;
        return this;
    }

    public long getLeaderHeartbeatPeriodInMillis() {
        return leaderHeartbeatPeriodInMillis;
    }

    public RaftConfig setLeaderHeartbeatPeriodInMillis(long leaderHeartbeatPeriodInMillis) {
        checkPositive(leaderHeartbeatPeriodInMillis, "leader heartbeat period in millis: "
                + leaderHeartbeatPeriodInMillis + " should be positive");
        this.leaderHeartbeatPeriodInMillis = leaderHeartbeatPeriodInMillis;
        return this;
    }

    public int getAppendRequestMaxEntryCount() {
        return appendRequestMaxEntryCount;
    }

    public RaftConfig setAppendRequestMaxEntryCount(int appendRequestMaxEntryCount) {
        checkPositive(appendRequestMaxEntryCount, "append request max entry count: " + appendRequestMaxEntryCount
                + " should be positive");
        this.appendRequestMaxEntryCount = appendRequestMaxEntryCount;
        return this;
    }

    public int getCommitIndexAdvanceCountToSnapshot() {
        return commitIndexAdvanceCountToSnapshot;
    }

    public RaftConfig setCommitIndexAdvanceCountToSnapshot(int commitIndexAdvanceCountToSnapshot) {
        checkPositive(commitIndexAdvanceCountToSnapshot, "commit index advance count to snapshot: "
                + commitIndexAdvanceCountToSnapshot + " should be positive");
        this.commitIndexAdvanceCountToSnapshot = commitIndexAdvanceCountToSnapshot;
        return this;
    }

    public int getUncommittedEntryCountToRejectNewAppends() {
        return uncommittedEntryCountToRejectNewAppends;
    }

    public RaftConfig setUncommittedEntryCountToRejectNewAppends(int uncommittedEntryCountToRejectNewAppends) {
        checkPositive(uncommittedEntryCountToRejectNewAppends, "uncommitted entry count to reject new appends: "
                + uncommittedEntryCountToRejectNewAppends + " should be positive");
        this.uncommittedEntryCountToRejectNewAppends = uncommittedEntryCountToRejectNewAppends;
        return this;
    }

    public boolean isFailOnIndeterminateOperationState() {
        return failOnIndeterminateOperationState;
    }

    public RaftConfig setFailOnIndeterminateOperationState(boolean failOnIndeterminateOperationState) {
        this.failOnIndeterminateOperationState = failOnIndeterminateOperationState;
        return this;
    }

    public boolean isAppendNopEntryOnLeaderElection() {
        return appendNopEntryOnLeaderElection;
    }

    public RaftConfig setAppendNopEntryOnLeaderElection(boolean appendNopEntryOnLeaderElection) {
        this.appendNopEntryOnLeaderElection = appendNopEntryOnLeaderElection;
        return this;
    }

    public List<RaftMember> getMembers() {
        return members;
    }

    public RaftConfig setMembers(List<RaftMember> m) {
        checkTrue(m.size() > 1, "Raft groups must have at least 2 members");

        members.clear();
        for (RaftMember member : m) {
            if (!members.contains(member)) {
                members.add(member);
            }
        }
        return this;
    }

    public int getMetadataGroupSize() {
        return metadataGroupSize;
    }

    public RaftConfig setMetadataGroupSize(int metadataGroupSize) {
        checkTrue(metadataGroupSize >= 2, "The metadata group must have at least 2 members");
        checkTrue(metadataGroupSize <= members.size(),
                "The metadata group cannot be bigger than the number of raft members");
        this.metadataGroupSize = metadataGroupSize;

        return this;
    }

    public List<RaftMember> getMetadataGroupMembers() {
        checkTrue(metadataGroupSize > 0, "metadata group size is not set");
        return members.subList(0, metadataGroupSize);
    }
}
