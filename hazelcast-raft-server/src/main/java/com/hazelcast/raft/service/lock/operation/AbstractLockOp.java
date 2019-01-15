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

package com.hazelcast.raft.service.lock.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.service.lock.LockEndpoint;
import com.hazelcast.raft.service.lock.RaftLockDataSerializerHook;
import com.hazelcast.raft.service.lock.RaftLockService;

import java.io.IOException;
import java.util.UUID;

/**
 * TODO: Javadoc Pending...
 */
abstract class AbstractLockOp extends RaftOp implements IdentifiedDataSerializable {

    String name;
    long sessionId;
    long threadId;
    UUID invocationUid;

    AbstractLockOp() {
    }

    AbstractLockOp(String name, long sessionId, long threadId, UUID invocationUid) {
        this.name = name;
        this.sessionId = sessionId;
        this.threadId = threadId;
        this.invocationUid = invocationUid;
    }

    LockEndpoint getLockEndpoint() {
        return new LockEndpoint(sessionId, threadId);
    }

    @Override
    public final String getServiceName() {
        return RaftLockService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return RaftLockDataSerializerHook.F_ID;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(name);
        out.writeLong(sessionId);
        out.writeLong(threadId);
        out.writeLong(invocationUid.getLeastSignificantBits());
        out.writeLong(invocationUid.getMostSignificantBits());
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        name = in.readUTF();
        sessionId = in.readLong();
        threadId = in.readLong();
        long least = in.readLong();
        long most = in.readLong();
        invocationUid = new UUID(most, least);
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", name=").append(name);
        sb.append(", sessionId=").append(sessionId);
        sb.append(", threadId=").append(threadId);
        sb.append(", invocationUid=").append(invocationUid);
    }
}
