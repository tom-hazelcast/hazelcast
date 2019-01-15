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

package com.hazelcast.raft.service.lock.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.Connection;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftGroupIdImpl;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.service.lock.RaftLockOwnershipState;
import com.hazelcast.raft.service.lock.RaftLockService;

import java.security.Permission;

/**
 * TODO: Javadoc Pending...
 */
public abstract class AbstractLockMessageTask extends AbstractMessageTask implements ExecutionCallback {

    protected RaftGroupId groupId;
    protected String name;
    protected long sessionId;

    AbstractLockMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object decodeClientMessage(ClientMessage clientMessage) {
        groupId = RaftGroupIdImpl.readFrom(clientMessage);
        name = clientMessage.getStringUtf8();
        sessionId = clientMessage.getLong();
        return null;
    }

    RaftInvocationManager getRaftInvocationManager() {
        RaftService raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
        return raftService.getInvocationManager();
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        if (response instanceof Integer) {
            return encodeLongResponse((Integer) response);
        }
        if (response instanceof Long) {
            return encodeLongResponse((Long) response);
        }
        if (response instanceof Boolean) {
            return encodeBooleanResponse((Boolean) response);
        }
        if (response instanceof RaftLockOwnershipState) {
            return encodeRaftLockOwnershipStateResponse((RaftLockOwnershipState) response);
        }

        throw new IllegalArgumentException("Unknown response: " + response);
    }

    private ClientMessage encodeLongResponse(long response) {
        int dataSize = ClientMessage.HEADER_SIZE + Bits.LONG_SIZE_IN_BYTES;
        ClientMessage clientMessage = ClientMessage.createForEncode(dataSize);
        clientMessage.set(response);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    private ClientMessage encodeBooleanResponse(boolean response) {
        int dataSize = ClientMessage.HEADER_SIZE + Bits.BOOLEAN_SIZE_IN_BYTES;
        ClientMessage clientMessage = ClientMessage.createForEncode(dataSize);
        clientMessage.set(response);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    private ClientMessage encodeRaftLockOwnershipStateResponse(RaftLockOwnershipState ownership) {
        ClientMessage clientMessage;
        int dataSize = ClientMessage.HEADER_SIZE + Bits.LONG_SIZE_IN_BYTES * 3 + Bits.INT_SIZE_IN_BYTES;
        clientMessage = ClientMessage.createForEncode(dataSize);
        clientMessage.set(ownership.getFence());
        clientMessage.set(ownership.getLockCount());
        clientMessage.set(ownership.getSessionId());
        clientMessage.set(ownership.getThreadId());

        return clientMessage;
    }

    @Override
    public void onResponse(Object response) {
        sendResponse(response);
    }

    @Override
    public void onFailure(Throwable t) {
        handleProcessingFailure(t);
    }

    @Override
    public String getServiceName() {
        return RaftLockService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }
}
