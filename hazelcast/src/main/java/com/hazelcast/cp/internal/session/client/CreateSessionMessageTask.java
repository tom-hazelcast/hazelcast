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

package com.hazelcast.cp.internal.session.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.session.SessionResponse;
import com.hazelcast.cp.internal.session.operation.CreateSessionOp;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.Connection;

import java.security.Permission;

import static com.hazelcast.cp.session.CPSession.CPSessionOwnerType.CLIENT;

/**
 * Client message task for {@link CreateSessionOp}
 */
public class CreateSessionMessageTask extends AbstractMessageTask implements ExecutionCallback {

    private CPGroupId groupId;

    private String endpointName;

    CreateSessionMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        RaftService service = nodeEngine.getService(RaftService.SERVICE_NAME);
        RaftInvocationManager invocationManager = service.getInvocationManager();
        RaftOp op = new CreateSessionOp(connection.getEndPoint(), endpointName, CLIENT, System.currentTimeMillis());
        invocationManager.invoke(groupId, op).andThen(this);
    }

    @Override
    protected Object decodeClientMessage(ClientMessage clientMessage) {
        groupId = RaftGroupId.readFrom(clientMessage);
        endpointName = clientMessage.getStringUtf8();
        return null;
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        if (response instanceof SessionResponse) {
            SessionResponse session = (SessionResponse) response;
            int dataSize = ClientMessage.HEADER_SIZE + Bits.LONG_SIZE_IN_BYTES * 3;
            ClientMessage clientMessage = ClientMessage.createForEncode(dataSize);
            clientMessage.set(session.getSessionId());
            clientMessage.set(session.getTtlMillis());
            clientMessage.set(session.getHeartbeatMillis());
            clientMessage.updateFrameLength();
            return clientMessage;
        }
        throw new IllegalArgumentException("Unknown response: " + response);
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
        return RaftService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
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
