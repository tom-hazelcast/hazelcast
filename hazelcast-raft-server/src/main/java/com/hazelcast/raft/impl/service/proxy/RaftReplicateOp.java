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

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.exception.NotLeaderException;
import com.hazelcast.raft.exception.RaftGroupDestroyedException;
import com.hazelcast.raft.impl.RaftSystemOperation;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * The base class that replicates the given {@link RaftOp} to the target raft group
 */
public abstract class RaftReplicateOp extends Operation
        implements IdentifiedDataSerializable, RaftSystemOperation, ExecutionCallback {

    private RaftGroupId groupId;

    public RaftReplicateOp() {
    }

    RaftReplicateOp(RaftGroupId groupId) {
        this.groupId = groupId;
    }

    @Override
    public final void run() {
        RaftService service = getService();
        RaftNode raftNode = service.getOrInitRaftNode(groupId);
        if (raftNode == null) {
            if (service.isRaftGroupDestroyed(groupId)) {
                sendResponse(new RaftGroupDestroyedException());
            } else {
                sendResponse(new NotLeaderException(groupId, service.getLocalMember(), null));
            }
            return;
        }

        ICompletableFuture future = replicate(raftNode);
        if (future == null) {
            return;
        }
        future.andThen(this);
    }

    ICompletableFuture replicate(RaftNode raftNode) {
        RaftOp op = getRaftOp();
        return raftNode.replicate(op);
    }

    @Override
    public void onResponse(Object response) {
        sendResponse(response);
    }

    @Override
    public void onFailure(Throwable t) {
        sendResponse(t);
    }

    protected abstract RaftOp getRaftOp();

    @Override
    public final boolean returnsResponse() {
        return false;
    }

    @Override
    public final boolean validatesTarget() {
        return false;
    }

    @Override
    public final String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(groupId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        groupId = in.readObject();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", groupId=").append(groupId);
    }
}
