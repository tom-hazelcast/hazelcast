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
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.service.MetadataRaftGroupManager;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.impl.InvocationTargetLeaveAware;

import java.io.IOException;

/**
 * Initiates the destroy process for the given Raft group.
 * <p/>
 * This operation is committed to the Metadata group.
 */
public class TriggerDestroyRaftGroupOp extends RaftOp implements InvocationTargetLeaveAware, IdentifiedDataSerializable {

    private RaftGroupId targetGroupId;

    public TriggerDestroyRaftGroupOp() {
    }

    public TriggerDestroyRaftGroupOp(RaftGroupId targetGroupId) {
        this.targetGroupId = targetGroupId;
    }

    // Please note that targetGroupId is the Raft group that is being queried and groupId argument is the Metadata Raft group
    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftService service = getService();
        MetadataRaftGroupManager metadataManager = service.getMetadataGroupManager();
        metadataManager.triggerDestroyRaftGroup(targetGroupId);
        return targetGroupId;
    }

    @Override
    public boolean isRetryableOnTargetLeave() {
        return true;
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
        return RaftServiceDataSerializerHook.TRIGGER_DESTROY_RAFT_GROUP_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(targetGroupId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        targetGroupId = in.readObject();
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", targetGroupId=").append(targetGroupId);
    }
}
