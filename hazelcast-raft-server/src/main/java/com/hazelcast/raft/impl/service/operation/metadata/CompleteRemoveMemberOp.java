package com.hazelcast.raft.impl.service.operation.metadata;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftMemberImpl;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.service.RaftMetadataManager;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.impl.util.Tuple2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class CompleteRemoveMemberOp extends RaftOp implements IdentifiedDataSerializable {

    private RaftMemberImpl member;

    private Map<RaftGroupId, Tuple2<Long, Long>> leftGroups;

    public CompleteRemoveMemberOp() {
    }

    public CompleteRemoveMemberOp(RaftMemberImpl member, Map<RaftGroupId, Tuple2<Long, Long>> leftGroups) {
        this.member = member;
        this.leftGroups = leftGroups;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftService service = getService();
        RaftMetadataManager metadataManager = service.getMetadataManager();
        metadataManager.completeRemoveMember(member, leftGroups);
        return member;
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(member);
        out.writeInt(leftGroups.size());
        for (Entry<RaftGroupId, Tuple2<Long, Long>> e : leftGroups.entrySet()) {
            out.writeObject(e.getKey());
            Tuple2<Long, Long> value = e.getValue();
            out.writeLong(value.element1);
            out.writeLong(value.element2);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        member = in.readObject();
        int count = in.readInt();
        leftGroups = new HashMap<RaftGroupId, Tuple2<Long, Long>>(count);
        for (int i = 0; i < count; i++) {
            RaftGroupId groupId = in.readObject();
            long currMembersCommitIndex = in.readLong();
            long newMembersCommitIndex = in.readLong();
            leftGroups.put(groupId, Tuple2.of(currMembersCommitIndex, newMembersCommitIndex));
        }
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.COMPLETE_REMOVE_MEMBER_OP;
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", member=").append(member);
        sb.append(", leftGroups=").append(leftGroups);
    }
}
