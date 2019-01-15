package com.hazelcast.raft.impl.service.operation.metadata;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.service.RaftMetadataManager;
import com.hazelcast.raft.operation.RaftOperation;
import com.hazelcast.raft.impl.service.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;

import java.io.IOException;

public class TriggerDestroyRaftGroupOperation extends RaftOperation implements IdentifiedDataSerializable {

    private RaftGroupId groupId;

    public TriggerDestroyRaftGroupOperation() {
    }

    public TriggerDestroyRaftGroupOperation(RaftGroupId groupId) {
        this.groupId = groupId;
    }

    @Override
    protected Object doRun(int commitIndex) {
        RaftService service = getService();
        RaftMetadataManager metadataManager = service.getMetadataManager();
        metadataManager.triggerDestroy(groupId);
        return groupId;
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        groupId.writeData(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        groupId = new RaftGroupId();
        groupId.readData(in);
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.TRIGGER_DESTROY_RAFT_GROUP_OP;
    }
}
