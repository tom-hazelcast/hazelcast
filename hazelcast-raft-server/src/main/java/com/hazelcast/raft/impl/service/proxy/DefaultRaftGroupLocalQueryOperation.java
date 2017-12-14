package com.hazelcast.raft.impl.service.proxy;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.operation.RaftOperation;

import java.io.IOException;

public class DefaultRaftGroupLocalQueryOperation extends RaftLocalQueryOperation {

    private RaftOperation raftOperation;

    public DefaultRaftGroupLocalQueryOperation() {
    }

    public DefaultRaftGroupLocalQueryOperation(RaftGroupId groupId, RaftOperation raftOperation) {
        super(groupId);
        this.raftOperation = raftOperation;
    }

    @Override
    protected RaftOperation getRaftOperation() {
        return raftOperation;
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
        out.writeObject(raftOperation);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        raftOperation = in.readObject();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", raftOp=").append(raftOperation);
    }
}
