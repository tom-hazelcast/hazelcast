package com.hazelcast.raft.service.atomiclong.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.atomiclong.AtomicLongDataSerializerHook;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLong;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 */
public class GetAndAddOp extends AbstractAtomicLongOp {

    private long delta;

    public GetAndAddOp() {
    }

    public GetAndAddOp(String name, long delta) {
        super(name);
        this.delta = delta;
    }

    @Override
    public Object doRun(RaftGroupId groupId, long commitIndex) {
        RaftAtomicLong atomic = getAtomicLong();
        return atomic.getAndAdd(delta);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(delta);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        delta = in.readLong();
    }

    @Override
    public int getId() {
        return AtomicLongDataSerializerHook.GET_AND_ADD_OP;
    }
}
