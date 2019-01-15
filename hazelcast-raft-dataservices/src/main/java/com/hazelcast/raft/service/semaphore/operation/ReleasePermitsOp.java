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

package com.hazelcast.raft.service.semaphore.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.semaphore.RaftSemaphoreService;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public class ReleasePermitsOp extends AbstractSemaphoreOp {

    private int sessionPermits;
    private int permits;

    public ReleasePermitsOp() {
    }

    public ReleasePermitsOp(String name, long sessionId, int sessionPermits, int permits) {
        super(name, sessionId);
        this.sessionPermits = sessionPermits;
        this.permits = permits;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) throws Exception {
        RaftSemaphoreService service = getService();
        return service.releasePermits(groupId, name, sessionId, sessionPermits, permits);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeInt(sessionPermits);
        out.writeInt(permits);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        sessionPermits = in.readInt();
        permits = in.readInt();
    }
}
