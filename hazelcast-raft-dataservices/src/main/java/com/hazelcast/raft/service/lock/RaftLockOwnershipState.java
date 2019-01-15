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

package com.hazelcast.raft.service.lock;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

import static com.hazelcast.raft.service.lock.RaftLockService.INVALID_FENCE;

/**
 * TODO: Javadoc Pending...
 */
public class RaftLockOwnershipState implements IdentifiedDataSerializable {

    static final RaftLockOwnershipState NOT_LOCKED
            = new RaftLockOwnershipState(INVALID_FENCE, 0, -1, -1);

    private long fence;

    private int lockCount;

    private long sessionId;

    private long threadId;

    public RaftLockOwnershipState() {
    }

    public RaftLockOwnershipState(long fence, int lockCount, long sessionId, long threadId) {
        this.fence = fence;
        this.lockCount = lockCount;
        this.sessionId = sessionId;
        this.threadId = threadId;
    }

    public boolean isLocked() {
        return fence != INVALID_FENCE;
    }

    public long getFence() {
        return fence;
    }

    public int getLockCount() {
        return lockCount;
    }

    public long getSessionId() {
        return sessionId;
    }

    public long getThreadId() {
        return threadId;
    }

    @Override
    public int getFactoryId() {
        return RaftLockDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftLockDataSerializerHook.RAFT_LOCK_OWNERSHIP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(fence);
        out.writeInt(lockCount);
        out.writeLong(sessionId);
        out.writeLong(threadId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        fence = in.readLong();
        lockCount = in.readInt();
        sessionId = in.readLong();
        threadId = in.readLong();
    }
}
