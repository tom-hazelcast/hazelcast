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

package com.hazelcast.raft.impl.service.proxy;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.InvocationTargetLeaveAware;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;

import java.io.IOException;

/**
 * The operation used by {@link RaftInvocationManager} to replicate a given {@link RaftOp} to leader of the target Raft group.
 * The leader sends the response for this operation after it commits the given operation to the majority of the Raft group.
 */
public class DefaultRaftReplicateOp extends RaftReplicateOp implements InvocationTargetLeaveAware {

    private RaftOp raftOp;

    public DefaultRaftReplicateOp() {
    }

    public DefaultRaftReplicateOp(RaftGroupId groupId, RaftOp raftOp) {
        super(groupId);
        this.raftOp = raftOp;
    }

    @Override
    protected RaftOp getRaftOp() {
        return raftOp;
    }

    @Override
    public boolean isRetryableOnTargetLeave() {
        if (raftOp instanceof InvocationTargetLeaveAware) {
            return ((InvocationTargetLeaveAware) raftOp).isRetryableOnTargetLeave();
        }

        return false;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.DEFAULT_RAFT_GROUP_REPLICATE_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(raftOp);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        raftOp = in.readObject();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", raftOp=").append(raftOp);
    }
}
