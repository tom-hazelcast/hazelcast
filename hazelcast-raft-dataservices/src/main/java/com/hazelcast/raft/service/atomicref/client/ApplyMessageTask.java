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

package com.hazelcast.raft.service.atomicref.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.service.atomicref.operation.ApplyOp;
import com.hazelcast.raft.service.atomicref.operation.ApplyOp.RETURN_VALUE_TYPE;

/**
 * TODO: Javadoc Pending...
 */
public class ApplyMessageTask extends AbstractAtomicRefMessageTask {

    private Data function;
    private RETURN_VALUE_TYPE returnValueType;
    private boolean alter;

    protected ApplyMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        RaftInvocationManager raftInvocationManager = getRaftInvocationManager();
        raftInvocationManager.invoke(groupId, new ApplyOp(name, function, returnValueType, alter)).andThen(this);
    }

    @Override
    protected Object decodeClientMessage(ClientMessage clientMessage) {
        super.decodeClientMessage(clientMessage);
        function = decodeNullableData(clientMessage);
        returnValueType = RETURN_VALUE_TYPE.valueOf(clientMessage.getStringUtf8());
        alter = clientMessage.getBoolean();

        return null;
    }
}
