/*
 *  Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.lock.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.cp.internal.datastructures.lock.RaftLockOwnershipState;
import com.hazelcast.cp.internal.datastructures.lock.RaftLockService;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.Connection;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.RaftService;

import java.security.Permission;

/**
 * Base class for client message tasks of Raft-based lock
 */
public abstract class AbstractLockMessageTask extends AbstractMessageTask implements ExecutionCallback {

    protected CPGroupId groupId;
    protected String name;
    protected long sessionId;

    AbstractLockMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object decodeClientMessage(ClientMessage clientMessage) {
        groupId = RaftGroupId.readFrom(clientMessage);
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
