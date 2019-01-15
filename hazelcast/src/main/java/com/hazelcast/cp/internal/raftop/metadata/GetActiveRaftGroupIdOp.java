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

package com.hazelcast.cp.internal.raftop.metadata;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;

import java.io.IOException;

/**
 * Returns {@link CPGroupId} of the given currently active Raft group
 * <p/>
 * This operation is committed to the Metadata group.
 */
public class GetActiveRaftGroupIdOp extends RaftOp implements IndeterminateOperationStateAware, IdentifiedDataSerializable {

    private String groupName;

    public GetActiveRaftGroupIdOp() {
    }

    public GetActiveRaftGroupIdOp(String groupName) {
        this.groupName = groupName;
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        RaftService service = getNodeEngine().getService(RaftService.SERVICE_NAME);
        return service.getMetadataGroupManager().getActiveRaftGroupId(groupName);
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return true;
    }

    @Override
    protected String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.GET_ACTIVE_RAFT_GROUP_ID_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(groupName);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        groupName = in.readUTF();
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", groupName=").append(groupName);
    }
}
