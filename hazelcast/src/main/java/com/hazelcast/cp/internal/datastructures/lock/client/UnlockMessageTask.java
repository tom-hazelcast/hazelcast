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

package com.hazelcast.cp.internal.datastructures.lock.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.datastructures.lock.operation.UnlockOp;

import java.util.UUID;

/**
 * Client message task for {@link UnlockOp}
 */
public class UnlockMessageTask extends AbstractLockMessageTask {

    private long threadId;
    private UUID invocationUid;
    private int lockCount;

    UnlockMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        RaftInvocationManager invocationManager = getRaftInvocationManager();
        invocationManager.invoke(groupId, new UnlockOp(name, sessionId, threadId, invocationUid, lockCount)).andThen(this);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return super.encodeResponse(response);
    }

    @Override
    protected Object decodeClientMessage(ClientMessage clientMessage) {
        super.decodeClientMessage(clientMessage);
        threadId = clientMessage.getLong();
        long least = clientMessage.getLong();
        long most = clientMessage.getLong();
        invocationUid = new UUID(most, least);
        lockCount = clientMessage.getInt();
        return null;
    }
}
