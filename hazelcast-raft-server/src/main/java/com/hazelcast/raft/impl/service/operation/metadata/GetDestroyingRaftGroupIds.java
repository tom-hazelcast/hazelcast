package com.hazelcast.raft.impl.service.operation.metadata;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.operation.RaftOperation;

public class GetDestroyingRaftGroupIds extends RaftOperation implements IdentifiedDataSerializable {

    public GetDestroyingRaftGroupIds() {
    }

    @Override
    protected Object doRun(int commitIndex) {
        RaftService service = getService();
        return service.getMetadataManager().getDestroyingRaftGroupIds();
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.GET_DESTROYING_RAFT_GROUP_IDS_OP;
    }

}
