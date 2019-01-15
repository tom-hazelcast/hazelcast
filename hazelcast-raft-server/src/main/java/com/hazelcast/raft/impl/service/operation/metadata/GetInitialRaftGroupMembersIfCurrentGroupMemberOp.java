/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.raft.impl.service.operation.metadata;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.RaftMember;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.impl.service.proxy.InvocationTargetLeaveAware;
import com.hazelcast.raft.impl.service.proxy.RaftNodeAware;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import static com.hazelcast.util.Preconditions.checkState;

/**
 * TODO: Javadoc Pending...
 */
public class GetInitialRaftGroupMembersIfCurrentGroupMemberOp extends RaftOp implements RaftNodeAware, InvocationTargetLeaveAware,
                                                                                        IdentifiedDataSerializable {

    private RaftMember raftMember;

    private RaftNode raftNode;

    public GetInitialRaftGroupMembersIfCurrentGroupMemberOp() {
    }

    public GetInitialRaftGroupMembersIfCurrentGroupMemberOp(RaftMember raftMember) {
        this.raftMember = raftMember;
    }

    @Override
    public void setRaftNode(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        checkState(raftNode != null, "RaftNode is not injected in " + groupId);
        Collection<RaftMember> members = raftNode.getCommittedMembers();
        checkState(members.contains(raftMember), raftMember
                + " is not in the current committed member list: " + members + " of " + groupId);
        return new ArrayList<RaftMember>(raftNode.getInitialMembers());
    }

    @Override
    public boolean isSafeToRetryOnTargetLeave() {
        return true;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.GET_INITIAL_RAFT_GROUP_MEMBERS_IF_CURRENT_GROUP_MEMBER_OP;
    }


    @Override
    protected String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(raftMember);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        raftMember = in.readObject();
    }
}
