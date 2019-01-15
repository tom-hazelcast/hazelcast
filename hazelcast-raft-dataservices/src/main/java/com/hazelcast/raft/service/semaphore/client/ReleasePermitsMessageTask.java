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

package com.hazelcast.raft.service.semaphore.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.service.semaphore.operation.ReleasePermitsOp;

import java.util.UUID;

/**
 * TODO: Javadoc Pending...
 */
public class ReleasePermitsMessageTask extends AbstractSemaphoreMessageTask {

    private UUID invocationUid;
    private int permits;

    ReleasePermitsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        RaftInvocationManager invocationManager = getRaftInvocationManager();
        invocationManager.invoke(groupId, new ReleasePermitsOp(name, sessionId, invocationUid, permits)).andThen(this);
    }

    @Override
    protected Object decodeClientMessage(ClientMessage clientMessage) {
        super.decodeClientMessage(clientMessage);
        long least = clientMessage.getLong();
        long most = clientMessage.getLong();
        invocationUid = new UUID(most, least);
        permits = clientMessage.getInt();
        return null;
    }
}
