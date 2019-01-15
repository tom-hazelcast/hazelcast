package com.hazelcast.raft.impl.service.proxy;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.QueryPolicy;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.exception.NotLeaderException;
import com.hazelcast.raft.exception.RaftGroupTerminatedException;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;

import java.io.IOException;

public class RaftQueryOp extends Operation implements IdentifiedDataSerializable, AllowedDuringPassiveState, ExecutionCallback {

    private RaftGroupId groupId;
    private QueryPolicy queryPolicy;
    private RaftOp raftOp;

    public RaftQueryOp() {
    }

    public RaftQueryOp(RaftGroupId groupId, RaftOp raftOp) {
        this.groupId = groupId;
        this.raftOp = raftOp;
    }

    @Override
    public final void run() {
        RaftService service = getService();
        RaftNode raftNode = service.getRaftNode(groupId);
        if (raftNode == null) {
            if (service.isRaftGroupDestroyed(groupId)) {
                sendResponse(new RaftGroupTerminatedException());
            } else {
                sendResponse(new NotLeaderException(groupId, service.getLocalMember(), null));
            }
            return;
        }

        ICompletableFuture future = raftNode.query(raftOp, queryPolicy);
        future.andThen(this);
    }

    public RaftQueryOp setQueryPolicy(QueryPolicy queryPolicy) {
        this.queryPolicy = queryPolicy;
        return this;
    }

    @Override
    public void onResponse(Object response) {
        sendResponse(response);
    }

    @Override
    public void onFailure(Throwable t) {
        sendResponse(t);
    }

    @Override
    public final boolean returnsResponse() {
        return false;
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }

    @Override
    public final String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.DEFAULT_RAFT_GROUP_QUERY_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(groupId);
        out.writeObject(raftOp);
        out.writeUTF(queryPolicy.toString());
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        groupId = in.readObject();
        raftOp = in.readObject();
        queryPolicy = QueryPolicy.valueOf(in.readUTF());
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", groupId=").append(groupId)
          .append(", policy=").append(queryPolicy);
    }
}
